package bss

import java.nio.file.Path

import akka.actor.{ Props, ActorLogging, Actor }
import bss.WatcherEvents.PathEvent

object EventsRouter {
  def props(rootPath: Path) = Props(classOf[EventsRouter], rootPath)
}

class EventsRouter(rootPath: Path) extends Actor with ActorLogging {

  def receive = {
    case PathEvent(path, eventType, count) =>
      log.info(s">>> $eventType ($count) : $path")
  }
}
