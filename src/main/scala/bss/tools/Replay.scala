package bss.tools

import java.io.FileOutputStream
import java.nio.file.{ Path, Paths }

import scala.io.Source
import scala.util.{ Failure, Success, Try }

object Replay extends App {

  if (args.size < 2) {
    println("Usage: Replay <events-file> <root-path> [<start>]")
    sys.exit(-1)
  }

  val eventsPath = Paths.get(args(0))
  val rootPath = Paths.get(args(1)).toAbsolutePath
  val start = if (args.size >= 3) args(2).toInt else 1

  println(s"Playing events from $eventsPath ...")

  val BclExtension = ".*\\.bcl(:?\\.gz)?".r
  val FileExtension = ".*\\.(:?tsv|xml|bin|txt)?".r

  var numLine = start

  for (line <- Source.fromFile(eventsPath.toFile).getLines().drop(start)) {
    line.split('\t') match {
      case Array(timestamp, sizeAsText, pathAsText, deltaAsText) =>
        Try(sizeAsText.toLong) match {
          case Success(sizeAsLong) =>
            val size = fileSizeAsText(sizeAsLong)
            val path = rootPath.resolve(pathAsText).normalize()
            val relPath = rootPath.relativize(path)
            val delta = deltaAsText.toLong
            val delayedSecs = delta / 1000000000.0f

            val startTime = System.nanoTime()

            def generateAndWait(generate: => Unit) = {
              println(f"$numLine%04d $timestamp $relPath ($size) [$delayedSecs%.1f s]")
              generate
              val waitTime = delta - (System.nanoTime() - startTime)
              if (waitTime > 0)
                Thread.sleep(waitTime / 1000000, (waitTime % 1000000).toInt)
            }

            relPath.toString match {
              case BclExtension(_) => generateAndWait { bclFile(path, sizeAsLong) }
              case FileExtension(_) => generateAndWait { randomFile(path, sizeAsLong) }
              case _ =>
            }

          case Failure(exc) if (exc.isInstanceOf[NumberFormatException]) =>
            println(s"Wrong file size at line $numLine: $line")
        }

      case _ =>
        println(s"Parse error at line $numLine: $line")
    }
    numLine += 1
  }

  def bclFile(path: Path, size: Long) = {
    // TODO meanwhile we just generate a random file
    randomFile(path, size)
  }

  def randomFile(path: Path, size: Long) = {
    val blockSize = 8 * 1024 * 1024 // 8 MiB
    val buffer = new Array[Byte](blockSize)
    val parentFile = path.getParent.toFile
    if (!parentFile.exists)
      parentFile.mkdirs()
    val os = new FileOutputStream(path.toFile)
    var wsize = size
    while (wsize > 0) {
      val len = math.min(blockSize, wsize).toInt
      scala.util.Random.nextBytes(buffer)
      os.write(buffer, 0, len)
      wsize -= len
      //printProgress(size - wsize, size)
    }
  }

  def printProgress(currentSize: Long, totalSize: Long) = {
    val pct = (currentSize * 100).toFloat / totalSize.toFloat
    print(f"$pct% 6.2f\r")
  }

  def fileSizeAsText(bytes: Long, si: Boolean = false): String = {
    val unit = if (si) 1000 else 1024
    if (bytes < unit)
      bytes + " B"
    else {
      val exp = (math.log(bytes) / math.log(unit)).toInt
      val pre = (if (si) "kMGTPE" else "KMGTPE").charAt(exp - 1) + (if (si) "" else "i")
      val s = bytes / math.pow(unit, exp)
      f"$s%.1f ${pre}B"
    }
  }
}
