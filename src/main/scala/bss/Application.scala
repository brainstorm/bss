package bss

import java.nio.file.Paths

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Application extends App {

  if (args.size != 1) {
    println("Usage: Application <path>")
    sys.exit(-1)
  }

  val rootPath = Paths.get(args(0)).toAbsolutePath
  //val rootPath = Paths.get(bssConfig.getString("root-path"))

  val config = ConfigFactory.load()

  val bssConfig = config.getConfig("bss")

  implicit val system = ActorSystem("Bss", config)

  sys.addShutdownHook {
    system.terminate()
  }

  println(s"Watching for ${rootPath.toAbsolutePath}")

  val router = system.actorOf(EventsRouter.props(rootPath), "EvtRouter")

  val dbBasePath = Some(Paths.get(bssConfig.getString("watcher.db-path")))

  system.actorOf(RecursiveWatcher.props(rootPath, router, dbBasePath = dbBasePath), "RecWatcher")

  Await.result(system.whenTerminated, Duration.Inf)
}
