package bss.watcher

import java.nio.file._
import java.security.MessageDigest

import scala.concurrent.duration.{ DurationDouble, FiniteDuration }
import scala.util.{ Failure, Success, Try }

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }

import bss.watcher.Watcher.EventType
import bss.watcher.WatcherActor.PathEvent

object WatcherActor {

  case class PathEvent(eventType: EventType, path: Path, isDirectory: Boolean, count: Int = 1)
  case class OverflowEvent()

  private[this] val msgDigest = MessageDigest.getInstance("MD5")
  def hash(input: Array[Byte]): Array[Byte] = MessageDigest.getInstance("MD5").digest(input)

  def props(
    rootPath: Path,
    listener: ActorRef,
    events: Seq[EventType] = Watcher.AllEventTypes,
    emptyPollInterval: FiniteDuration = 0.5 seconds,
    dbBasePath: Option[Path] = None
  ) =
    Props(classOf[WatcherActor], rootPath, listener, events, emptyPollInterval,
      dbBasePath.getOrElse(Paths.get(".").toAbsolutePath().normalize))
}

class WatcherActor(
  rootPath: Path,
  listener: ActorRef,
  events: Seq[EventType],
  emptyPollInterval: FiniteDuration,
  dbBasePath: Path
)
    extends Actor with ActorLogging {

  private[this] case class Poll()

  private[this] val kinds = events.map(_.kind)

  //private[this] var watcher: WatchService = _
  private[this] var watcher: Watcher = _
  private[this] var db: WatcherDb = _

  override def preStart() = {
    // Initialize the watch service
    log.info("Creating the watch service ...")
    Try(Watcher(rootPath)) match {
      case Success(value) =>
        watcher = value
        Try(watcher.register(rootPath)) match {
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
    watcher.recover(db) { (eventType, path, isDirectory) =>
      notifyPathEvent(eventType, path, isDirectory)
    }
  }

  def poll() = {
    def notifyOverflow() = {
      log.debug("Watch service overflow")
      context.become(recovering)
    }

    watcher.poll(db)(notifyPathEvent, notifyOverflow, { s => log.debug(s) }) match {
      case true =>
        self ! Poll

      case false =>
        import scala.concurrent.ExecutionContext.Implicits.global
        context.system.scheduler.scheduleOnce(emptyPollInterval, self, Poll)
    }
  }

  def notifyPathEvent(eventType: EventType, path: Path, isDirectory: Boolean /*, count: Int = 1*/ ) = {
    listener ! PathEvent(eventType, path, isDirectory)
  }
}
