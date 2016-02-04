package bss.watcher

import java.nio.file.StandardWatchEventKinds._
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes

import bss.watcher.Watcher._

object Watcher {

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

  type NotifyPathCallback = (EventType, Path, Boolean) => Unit

  def apply(rootPath: Path, events: Seq[EventType] = AllEventTypes) = new Watcher(rootPath, events)
}

class Watcher(val rootPath: Path, events: Seq[EventType]) {

  val kinds = events.map(_.kind)

  val watchService = FileSystems.getDefault.newWatchService()

  rootPath.register(watchService, kinds: _*)

  def close() = watchService.close()

  def register(path: Path) = path.register(watchService, kinds: _*)

  def recover(db: WatcherDb, fromPath: Path = rootPath)(notifyPathEvent: NotifyPathCallback) = {

    def walkPath(path: Path, attrs: BasicFileAttributes, register: Boolean = false): FileVisitResult = {
      require(path != null)
      require(attrs != null)

      if (path != fromPath) {
        if (register)
          path.register(watchService, kinds: _*)

        db.checkRecover(path, attrs)(notifyPathEvent)
      }

      FileVisitResult.CONTINUE
    }

    Files.walkFileTree(fromPath, new SimpleFileVisitor[Path] {
      override def preVisitDirectory(path: Path, attrs: BasicFileAttributes): FileVisitResult =
        walkPath(path, attrs, register = true)

      override def visitFile(path: Path, attrs: BasicFileAttributes): FileVisitResult =
        walkPath(path, attrs)
    })

    db.flush(false)
  }

  def snapshot(db: WatcherDb)(notifyPathEvent: NotifyPathCallback = { (_, _, _) => }) = {

    def walkPath(path: Path, attrs: BasicFileAttributes): FileVisitResult = {
      require(path != null)
      require(attrs != null)

      if (path != rootPath)
        db.checkRecover(path, attrs)(notifyPathEvent)

      FileVisitResult.CONTINUE
    }

    Files.walkFileTree(rootPath, new SimpleFileVisitor[Path] {
      override def preVisitDirectory(path: Path, attrs: BasicFileAttributes): FileVisitResult = walkPath(path, attrs)
      override def visitFile(path: Path, attrs: BasicFileAttributes): FileVisitResult = walkPath(path, attrs)
    })

    db.flush(false)
  }

  def poll(db: WatcherDb)(notifyPathEvent: NotifyPathCallback, notifyOverflow: () => Unit,
    debug: => (String => Unit)): Boolean = {
    import collection.JavaConverters._

    def absolutePath(key: WatchKey, path: Path) = {
      val parentPath = key.watchable().asInstanceOf[Path]
      parentPath.resolve(path).toAbsolutePath.normalize()
    }

    val key = watchService.poll()
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

        debug(s"==> $kindName ($count) : $ctxClassName [$ctxRepr]")

        val eventType = EventType(kind)
        eventType match {
          case Create | Modify | Delete =>
            val path = absolutePath(key, ctx.asInstanceOf[Path])
            val attrs = Files.readAttributes(path, classOf[BasicFileAttributes])

            if (eventType == Create && attrs.isDirectory) {
              val wk = register(path)
              debug(s"--> Registered $ctxRepr")
              recover(db, path)(notifyPathEvent)
            }

            db.update(path, attrs)

            notifyPathEvent(eventType, path, attrs.isDirectory) //, count)

          case Overflow =>
            notifyOverflow()

          case _ =>
            debug(s"Unknown event type: $kindName")
        }
      }
      key.reset()

      true
    } else
      false
  }
}
