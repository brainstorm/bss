package bss

import java.nio.file.Path

import akka.actor.{ Props, ActorLogging, Actor }
import bss.watcher.WatcherActor
import WatcherActor.PathEvent

object EventsRouter {
  def props(rootPath: Path) = Props(classOf[EventsRouter], rootPath)
}

class EventsRouter(rootPath: Path) extends Actor with ActorLogging {

  def receive = {
    case PathEvent(eventType, path, isDirectory, count) =>
      val relPath = rootPath.relativize(path)
      log.info(s">>> $eventType : $relPath")
  }
}
