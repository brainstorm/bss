package bss

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.security.MessageDigest

import bss.WatcherEvents.{ Create, Modify }
import org.rocksdb.{ FlushOptions, Options, RocksDB }

object WatcherDb {
  def apply(watcherRootPath: Path, dbBasePath: Path) = new WatcherDb(watcherRootPath, dbBasePath)
}

class WatcherDb(watcherRootPath: Path, dbBasePath: Path) {

  private[this] val db: RocksDB = {
    val options = new Options().setCreateIfMissing(true)
    RocksDB.open(options, dbBasePath.toString)
  }

  def hash(input: Array[Byte]): Array[Byte] = MessageDigest.getInstance("MD5").digest(input)

  def pathNameHash(path: Path): Array[Byte] = {
    hash(path.toString.getBytes(StandardCharsets.UTF_8))
  }

  def pathAttrsHash(lastModifiedMillis: Long, size: Long): Array[Byte] = {
    hash(ByteBuffer.allocate(16)
      .putLong(lastModifiedMillis)
      .putLong(size)
      .array())
  }

  def update(path: Path, attrs: BasicFileAttributes) = {
    val pathKey = pathNameHash(watcherRootPath.relativize(path))
    val attrsHash = pathAttrsHash(attrs.lastModifiedTime.toMillis, attrs.size)
    db.put(pathKey, attrsHash)
  }

  def checkRecover(path: Path, attrs: BasicFileAttributes)(
    notifyPathEvent: (WatcherEvents.EventType, Path, Boolean) => Unit
  ): Unit = {

    val pathKey = pathNameHash(watcherRootPath.relativize(path))
    val attrsHash = pathAttrsHash(attrs.lastModifiedTime.toMillis, attrs.size)

    Option(db.get(pathKey)) match {
      case Some(prevAttrsHash) =>
        if (!prevAttrsHash.sameElements(attrsHash)) {
          notifyPathEvent(Modify, path, attrs.isDirectory)
          db.put(pathKey, attrsHash)
        }
      case None =>
        notifyPathEvent(Create, path, attrs.isDirectory)
        db.put(pathKey, attrsHash)
    }
  }

  def recover(watcher: WatchService, events: Seq[WatcherEvents.EventType])(
    notifyPathEvent: (WatcherEvents.EventType, Path, Boolean) => Unit
  ) = {

    val kinds = events.map(_.kind)

    def walkPath(path: Path, attrs: BasicFileAttributes, register: Boolean = false): FileVisitResult = {
      require(path != null)
      require(attrs != null)

      if (path != watcherRootPath) {
        if (register)
          path.register(watcher, kinds: _*)

        checkRecover(path, attrs)(notifyPathEvent)
      }

      FileVisitResult.CONTINUE
    }

    Files.walkFileTree(watcherRootPath, new SimpleFileVisitor[Path] {
      override def preVisitDirectory(path: Path, attrs: BasicFileAttributes): FileVisitResult =
        walkPath(path, attrs, register = true)

      override def visitFile(path: Path, attrs: BasicFileAttributes): FileVisitResult =
        walkPath(path, attrs)
    })

    db.flush(new FlushOptions().setWaitForFlush(false))
  }

  def snapshot(notifyPathEvent: (WatcherEvents.EventType, Path, Boolean) => Unit = { (_, _, _) => }) = {

    def walkPath(path: Path, attrs: BasicFileAttributes): FileVisitResult = {
      require(path != null)
      require(attrs != null)

      if (path != watcherRootPath)
        checkRecover(path, attrs)(notifyPathEvent)

      FileVisitResult.CONTINUE
    }

    Files.walkFileTree(watcherRootPath, new SimpleFileVisitor[Path] {
      override def preVisitDirectory(path: Path, attrs: BasicFileAttributes): FileVisitResult = walkPath(path, attrs)
      override def visitFile(path: Path, attrs: BasicFileAttributes): FileVisitResult = walkPath(path, attrs)
    })

    db.flush(new FlushOptions().setWaitForFlush(false))
  }

  def close() = {
    db.close()
  }
}
