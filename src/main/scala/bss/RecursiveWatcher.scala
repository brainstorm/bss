package bss

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file._
import java.nio.file.StandardWatchEventKinds._
import java.nio.file.attribute.BasicFileAttributes

import java.security.MessageDigest

import bss.WatcherEvents._
import org.rocksdb.{ FlushOptions, Options, RocksDB }

import collection.JavaConverters._
import akka.actor.{ ActorRef, Props, ActorLogging, Actor }

import scala.concurrent.duration.{ DurationDouble, FiniteDuration }
import scala.util.{ Failure, Success, Try }

object WatcherEvents {

  sealed trait EventType { def kind: WatchEvent.Kind[_] }
  case object Create extends EventType { val kind = ENTRY_CREATE }
  case object Modify extends EventType { val kind = ENTRY_MODIFY }
  case object Delete extends EventType { val kind = ENTRY_DELETE }
  case object Overflow extends EventType { val kind = OVERFLOW }
  case class Unknown(val kind: WatchEvent.Kind[_]) extends EventType

  object EventType {
    def apply(kind: WatchEvent.Kind[_]): EventType = kind.name match {
      case "ENTRY_CREATE" => Create
      case "ENTRY_MODIFY" => Modify
      case "ENTRY_DELETE" => Delete
      case "OVERFLOW" => Overflow
      case _ => Unknown(kind) //new EventType { val kind = kind }
    }
  }

  val AllEventTypes = Seq(Create, Modify, Delete)

  case class PathEvent(eventType: EventType, path: Path, isDirectory: Boolean, count: Int = 1)
  case class OverflowEvent()
}

object RecursiveWatcher {

  private[this] val msgDigest = MessageDigest.getInstance("MD5")
  def hash(input: Array[Byte]): Array[Byte] = MessageDigest.getInstance("MD5").digest(input)

  def props(
    rootPath: Path,
    listener: ActorRef,
    events: Seq[EventType] = AllEventTypes,
    emptyPollInterval: FiniteDuration = 0.5 seconds,
    dbBasePath: Option[Path] = None
  ) =
    Props(classOf[RecursiveWatcher], rootPath, listener, events, emptyPollInterval,
      dbBasePath.getOrElse(Paths.get(".").toAbsolutePath().normalize))
}

class RecursiveWatcher(
  rootPath: Path,
  listener: ActorRef,
  events: Seq[WatcherEvents.EventType],
  emptyPollInterval: FiniteDuration,
  dbBasePath: Path
)
    extends Actor with ActorLogging {

  private[this] case class Poll()

  private[this] val kinds = events.map(_.kind)

  private[this] var watcher: WatchService = _
  private[this] var db: RocksDB = _

  override def preStart() = {
    // Initialize the watch service
    watcher = FileSystems.getDefault.newWatchService()
    rootPath.register(watcher, kinds: _*)

    // Initialize the database
    val options = new Options().setCreateIfMissing(true)
    Try(RocksDB.open(options, dbBasePath.toString)) match {
      case Success(value) =>
        db = value
        self ! Poll

      case Failure(exception) =>
        log.error(s"Exception while opening the database at '$dbBasePath': $exception")
        context.system.terminate()
    }
  }

  def receive = recovering

  def recovering: Receive = {
    case Poll =>
      log.info("Recovering state ...")
      recover()
      log.info("Polling events ...")
      context.become(polling)
      self ! Poll
  }

  def polling: Receive = {
    case Poll =>
      poll()
  }

  def recover() = {
    def walkPath(path: Path, attrs: BasicFileAttributes, register: Boolean = false): FileVisitResult = {
      require(path != null)
      require(attrs != null)

      if (path != rootPath) {
        if (register)
          path.register(watcher, kinds: _*)

        dbCheckRecover(db, path, attrs)
      }

      FileVisitResult.CONTINUE
    }

    try {
      Files.walkFileTree(rootPath, new SimpleFileVisitor[Path] {
        override def preVisitDirectory(path: Path, attrs: BasicFileAttributes): FileVisitResult =
          walkPath(path, attrs, register = true)

        override def visitFile(path: Path, attrs: BasicFileAttributes): FileVisitResult =
          walkPath(path, attrs)

        //override def postVisitDirectory(path: Path, exc: IOException): FileVisitResult = {
        //  if (exc != null) throw exc
        //  FileVisitResult.CONTINUE
        //}
      })
    } catch {
      case e: IOException =>
        log.warning(s"Exception while traversing the paths for recovery: $e")
    }

    db.flush(new FlushOptions().setWaitForFlush(false))
  }

  def poll() = {
    val key = watcher.poll()
    if (key != null) {
      for (event <- key.pollEvents().asScala) {
        val kind = event.kind()
        val kindName = kind.name()
        val count = event.count()
        val ctx = event.context()
        val ctxClassName = ctx.getClass.getCanonicalName
        val ctxRepr = ctx match {
          case path: Path => rootPath.relativize(absolutePath(key, path)).toString
          case _ => ctx.toString
        }

        log.debug(s"==> $kindName ($count) : $ctxClassName [$ctxRepr]")

        val eventType = EventType(kind)
        eventType match {
          case Create | Modify | Delete =>
            val path = absolutePath(key, ctx.asInstanceOf[Path])
            val attrs = Files.readAttributes(path, classOf[BasicFileAttributes])

            if (eventType == Create && attrs.isDirectory) {
              val wk = path.register(watcher, kinds: _*)
              log.debug(s"--> Registered $ctxRepr")
            }

            dbUpdate(db, path, attrs)

            notifyPathEvent(eventType, path, attrs.isDirectory, count)

          case Overflow =>
            log.debug("Watch service overflow")
            context.become(recovering)

          case _ =>
            log.debug(s"Unknown event type: $kindName")
        }
      }
      key.reset()
      self ! Poll
    } else {
      import scala.concurrent.ExecutionContext.Implicits.global
      context.system.scheduler.scheduleOnce(emptyPollInterval, self, Poll)
    }
  }

  def pathNameHash(path: Path): Array[Byte] = {
    RecursiveWatcher.hash(path.toString.getBytes(StandardCharsets.UTF_8))
  }

  def pathAttrsHash(lastModifiedMillis: Long, size: Long): Array[Byte] = {
    RecursiveWatcher.hash(
      ByteBuffer.allocate(16)
        .putLong(lastModifiedMillis)
        .putLong(size)
        .array()
    )
  }

  def dbCheckRecover(db: RocksDB, path: Path, attrs: BasicFileAttributes): Unit = {
    val pathKey = pathNameHash(path)
    val attrsHash = pathAttrsHash(attrs.lastModifiedTime.toMillis, attrs.size)

    Option(db.get(pathKey)) match {
      case Some(prevAttrsHash) =>
        if (!prevAttrsHash.sameElements(attrsHash)) {
          notifyPathEvent(Modify, path, attrs.isDirectory)
          db.put(pathKey, attrsHash)
        }
      case None =>
        notifyPathEvent(Create, path, attrs.isDirectory)
        db.put(pathKey, attrsHash)
    }
  }

  def dbUpdate(db: RocksDB, path: Path, attrs: BasicFileAttributes) = {
    val pathKey = pathNameHash(path)
    val attrsHash = pathAttrsHash(attrs.lastModifiedTime.toMillis, attrs.size)
    db.put(pathKey, attrsHash)
  }

  def notifyPathEvent(eventType: EventType, path: Path, isDirectory: Boolean, count: Int = 1) = {
    listener ! PathEvent(eventType, path, isDirectory, count)
  }

  def absolutePath(key: WatchKey, path: Path) = {
    val parentPath = key.watchable().asInstanceOf[Path]
    parentPath.resolve(path).toAbsolutePath.normalize()
  }
}
