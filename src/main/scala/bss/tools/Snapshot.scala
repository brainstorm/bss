package bss.tools

import java.nio.file._

import bss.watcher.{ Watcher, WatcherDb }
import com.typesafe.config.ConfigFactory

object Snapshot extends App {

  if (args.size != 1) {
    println("Usage: Snapshot <path>")
    sys.exit(-1)
  }

  val rootPath = Paths.get(args(0)).toAbsolutePath

  val config = ConfigFactory.load()

  val dbBasePath = Paths.get(config.getString("bss.watcher.db-path"))

  val db = WatcherDb(rootPath, dbBasePath)
  Watcher(rootPath).snapshot(db) { (eventType, path, isDirectory) =>
    println(s"--> $eventType $path")
  }
}
