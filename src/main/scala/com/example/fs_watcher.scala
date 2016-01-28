package com.example

import akka.actor.ActorSystem
import com.beachape.filemanagement.RxMonitor

import java.nio.file.Paths
import java.nio.file.StandardWatchEventKinds._

object sch_fs_watcher {
  def main(args: Array[String]): Unit = {

    implicit val system = ActorSystem("actorSystem")

    val monitor = RxMonitor()
    val observable = monitor.observable

    val subscription = observable.subscribe(
      onNext = { p => println(s"The instrument has written a file!: $p")},
      onError = { t => println(t)},
      onCompleted = { () => println("File sent to common sequencing bus") }
    )

    val flowcells = Paths get "/Users/romanvg/tiny-test-data/flowcell"

    monitor.registerPath(ENTRY_CREATE, flowcells)

    //Thread.sleep(100)

    //modify a monitored file
    //val writer = new BufferedWriter(new FileWriter(flowcells.toFile))
    //writer.write("Theres text in here wee!!")
    //writer.close

    // #=> Something was modified in a file mufufu: /Users/lloyd/Desktop/test

    // stop monitoring
    monitor.stop()

    // #=> Monitor has been shut down
  }
}
