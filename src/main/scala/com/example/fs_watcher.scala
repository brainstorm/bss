package com.example

import akka.actor.ActorSystem
import com.beachape.filemanagement.{MonitorActor, RxMonitor}

import java.nio.file.Paths
import java.nio.file.StandardWatchEventKinds._

object sch_fs_watcher {
  def main(args: Array[String]): Unit = {

    implicit val system = ActorSystem("actorSystem")
    val fileMonitorActor = system.actorOf(MonitorActor(concurrency = 2))

    val monitor = RxMonitor()
    val observable = monitor.observable

    val subscription = observable.subscribe(
      onNext = { p => println(s"The instrument has written a file!: $p")},
      onError = { t => println(t)},
      onCompleted = { () => println("File sent to common sequencing bus") }
    )

    //val flowcells = Paths get "/Users/romanvg/tiny-test-data/flowcell"

    val flowcells = Paths get "/Users/romanvg/Desktop/flowcell"

    monitor.registerPath(ENTRY_MODIFY, flowcells)

    //monitor.stop()
  }
}