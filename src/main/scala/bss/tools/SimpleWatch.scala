package bss.tools

import java.nio.file.StandardWatchEventKinds.{ ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY }
import java.nio.file._

import scala.collection.JavaConverters._

object SimpleWatch extends App {

  if (args.size != 1) {
    println("Usage: WatchTest <path>")
    sys.exit(-1)
  }

  val rootPath = Paths.get(args(0)).toAbsolutePath
  println(s"Watching $rootPath ...")

  val watcher = FileSystems.getDefault.newWatchService()
  val events = Seq(ENTRY_CREATE, ENTRY_MODIFY, ENTRY_DELETE)
  val wk = rootPath.register(watcher, events: _*)

  while (true) {
    val maybeKey = Option(watcher.poll())
    if (maybeKey.isDefined) {
      val key = maybeKey.get
      for (event <- key.pollEvents().asScala) {
        val kind = event.kind()
        val kindName = kind.name()
        val count = event.count()
        val context = event.context()
        val contextClassName = context.getClass.getCanonicalName
        val contextRepr = context match {
          case path: Path => rootPath.relativize(absolutePath(key, path)).toString
          case _ => context.toString
        }

        println(s"==> $kindName ($count) : $contextClassName [$contextRepr]")

        if (kindName == "ENTRY_CREATE") {
          val path = absolutePath(key, context.asInstanceOf[Path])
          if (path.toFile.isDirectory) {
            val wk = path.register(watcher, events: _*)
            println(s"--> Registered $contextRepr")
          }
        }
      }
      key.reset()
    } else
      Thread.sleep(500)
  }

  def absolutePath(key: WatchKey, path: Path) = {
    val parentPath = key.watchable().asInstanceOf[Path]
    parentPath.resolve(path).toAbsolutePath
  }
}
