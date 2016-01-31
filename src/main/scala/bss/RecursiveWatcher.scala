package bss

import java.nio.file._
import java.nio.file.StandardWatchEventKinds._

import bss.WatcherEvents._

import collection.JavaConverters._
import akka.actor.{ ActorRef, Props, ActorLogging, Actor }

import scala.concurrent.duration.{ DurationDouble, FiniteDuration }

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

  case class PathEvent(path: Path, eventType: EventType, count: Int)
  case class OverflowEvent()
}

object RecursiveWatcher {

  def props(
    rootPath: Path,
    listener: ActorRef,
    events: Seq[EventType] = AllEventTypes,
    emptyPollInterval: FiniteDuration = 0.5 seconds
  ) =
    Props(classOf[RecursiveWatcher], rootPath, listener, events, emptyPollInterval)
}

class RecursiveWatcher(
  rootPath: Path,
  listener: ActorRef,
  events: Seq[WatcherEvents.EventType],
  emptyPollInterval: FiniteDuration
)
    extends Actor with ActorLogging {

  private[this] case class Poll()

  val kinds = events.map(_.kind)

  var watcher: WatchService = _
  var subscribers = Map.empty[Path, ActorRef]

  override def preStart() = {
    watcher = FileSystems.getDefault.newWatchService()
    rootPath.register(watcher, kinds: _*)
    self ! Poll
  }

  def receive = polling

  def recovering: Receive = {
    // TODO recovering not implemented yet
    case Poll =>
      context.become(polling)
      self ! Poll
  }

  def polling: Receive = {
    case Poll =>
      poll()
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
          case Create =>
            val path = absolutePath(key, ctx.asInstanceOf[Path])
            if (path.toFile.isDirectory) {
              val wk = path.register(watcher, kinds: _*)
              log.debug(s"--> Registered $ctxRepr")
            }
            notifyPathEvent(path, eventType, count)
          case Modify | Delete =>
            val path = absolutePath(key, ctx.asInstanceOf[Path])
            notifyPathEvent(path, eventType, count)

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

  def notifyPathEvent(path: Path, eventType: EventType, count: Int) = {
    listener ! PathEvent(path, eventType, count)
  }

  def absolutePath(key: WatchKey, path: Path) = {
    val parentPath = key.watchable().asInstanceOf[Path]
    parentPath.resolve(path).toAbsolutePath
  }
}
