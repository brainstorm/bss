package bss

import java.nio.file.StandardWatchEventKinds._
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.security.MessageDigest

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import bss.WatcherEvents._

import scala.collection.JavaConverters._
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
  private[this] var db: WatcherDb = _

  override def preStart() = {
    // Initialize the watch service
    log.info("Creating the watch service ...")
    Try(FileSystems.getDefault.newWatchService()) match {
      case Success(value) =>
        watcher = value
        Try(rootPath.register(watcher, kinds: _*)) match {
          case Failure(exception) =>
            log.error(s"Exception while registering for '$rootPath': $exception")
            context.system.terminate()
          case _ =>
        }

      case Failure(exception) =>
        log.error(s"Exception while creating the watch service for '$rootPath': $exception")
        context.system.terminate()
    }

    // Initialize the database
    log.info(s"Opening RocksDB at $dbBasePath ...")
    Try(WatcherDb(rootPath, dbBasePath)) match {
      case Success(value) =>
        db = value
        self ! Poll

      case Failure(exception) =>
        log.error(s"Exception while opening the database at '$dbBasePath': $exception")
        context.system.terminate()
    }
  }

  override def postStop() = {
    log.info("Closing the watch service ...")
    Try(watcher.close()) match {
      case Failure(exception) if !exception.isInstanceOf[NullPointerException] =>
        log.error(s"Exception while closing the watcher service: $exception")
      case _ =>
    }

    log.info("Closing RocksDB ...")
    Try(db.close()) match {
      case Failure(exception) if !exception.isInstanceOf[NullPointerException] =>
        log.error(s"Exception while closing RocksDB: $exception")
      case _ =>
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
    db.recover(watcher, events) { (eventType, path, isDirectory) =>
      notifyPathEvent(eventType, path, isDirectory)
    }
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

            db.update(path, attrs)

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

  def notifyPathEvent(eventType: EventType, path: Path, isDirectory: Boolean, count: Int = 1) = {
    listener ! PathEvent(eventType, path, isDirectory, count)
  }

  def absolutePath(key: WatchKey, path: Path) = {
    val parentPath = key.watchable().asInstanceOf[Path]
    parentPath.resolve(path).toAbsolutePath.normalize()
  }
}
