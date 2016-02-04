package bss.tools

import java.nio.file.{ Path, Paths }

import scala.concurrent.Await
import scala.concurrent.duration.Duration

import com.typesafe.config.ConfigFactory

import akka.actor.ActorSystem

import bss.EventsRouter
import bss.watcher.WatcherActor

object Watch extends App {

  val config = ConfigFactory.load()

  val bssConfig = config.getConfig("bss")

  val rootPath = if (args.size > 0)
    Paths.get(args(0)).toAbsolutePath
  else
    Paths.get(bssConfig.getString("root-path"))

  implicit val system = ActorSystem("Bss", config)

  sys.addShutdownHook {
    if (!system.whenTerminated.isCompleted)
      system.terminate()
  }

  println(s"Watching for ${rootPath.toAbsolutePath}")

  val router = system.actorOf(EventsRouter.props(rootPath), "EvtRouter")

  val dbBasePath = Some(Paths.get(bssConfig.getString("watcher.db-path")))

  system.actorOf(WatcherActor.props(rootPath, router, dbBasePath = dbBasePath), "RecWatcher")

  Await.result(system.whenTerminated, Duration.Inf)
}
