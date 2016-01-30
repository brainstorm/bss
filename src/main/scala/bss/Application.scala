package bss

import java.nio.file.Paths
import java.nio.file.StandardWatchEventKinds._

import akka.actor.ActorSystem
import com.beachape.filemanagement.Messages.RegisterCallback
import com.beachape.filemanagement.MonitorActor
import com.typesafe.config.ConfigFactory
//import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.concurrent.duration.{ Duration, DurationDouble, DurationInt }

object Application extends App {

  //val logger = LoggerFactory.getLogger(this.getClass)

  val config = ConfigFactory.load()

  val bssConfig = config.getConfig("bss")

  implicit val system = ActorSystem("Bss", config)

  val monitorConcurrency = bssConfig.getInt("monitor-concurrency")

  val fileMonitorActor = system.actorOf(MonitorActor(
    dedupeTime = 1 second,
    concurrency = monitorConcurrency
  ))

  val rootPath = Paths.get(bssConfig.getString("root-path"))

  println(s"Watching for ${rootPath.toAbsolutePath}")

  fileMonitorActor ! RegisterCallback(
    ENTRY_CREATE,
    path = rootPath,
    recursive = true,
    callback = { path => println(s"==> $path") }
  )

  Await.result(system.whenTerminated, Duration.Inf)
}
