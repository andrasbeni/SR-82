package com.github.andrasbeni.rq

import java.io.{ByteArrayOutputStream, DataOutputStream, RandomAccessFile}
import java.nio.ByteBuffer
import java.util
import java.util.Properties

import com.github.andrasbeni.rq.proto.{AddOrRemove, LogEntry, Operation}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.immutable.IndexedSeq

class Persistence(
      val config : Properties)(
      val log : Log = new Log(
        config.getProperty("log.file"),
        new LogEntry(0L, 0L, new AddOrRemove(Operation.Add, ZeroBytes()))),
      val state : State = new State(
        config.getProperty("state.file"),
        VoteAndTerm(-1, 0))) extends AutoCloseable {

  def entriesFrom(fromIndex: Long): IndexedSeq[LogEntry] =
    for (index <- fromIndex to log.lastEntry.getIndex)
      yield log.read(index).get


  val logger : Logger = LoggerFactory.getLogger( classOf[Persistence] )
  var commitIndex : Long = 0
  var lastApplied : Long = 0

  def setVoteAndTerm(vote: Int, term : Long): Unit = {
    state.set(VoteAndTerm(vote, term))
  }

  def term : Long = state.get().term
  def voteAndTerm : VoteAndTerm = state.get()



  override def close() : Unit = {
    List(log, state).foreach( c =>
    try
      c.close()
    catch {
      case e: Exception  => logger.error(s"Error closing ${c.getClass.getSimpleName}", e)
    })
  }

}

class Log(fileName : String, defaultLastEntry : LogEntry) extends AutoCloseable {

  val logger : Logger = LoggerFactory.getLogger(classOf[Log])

  val file : RandomAccessFile = new RandomAccessFile(fileName, "rws")
  val indexMap = new util.TreeMap[Long, Long]();
  {
    indexMap.put(0, Long.MinValue)
    file.seek(0)
    while (file.getFilePointer < file.length()) {
      val pointer = file.getFilePointer
      val t : LogEntry = LogEntryHelper.read(file)
      indexMap.put(t.getIndex, pointer)
    }
  }
  def containsIndex(index : Long) : Boolean = {
    indexMap.containsKey(index)
  }
  def read(index : Long) : Option[LogEntry] = {
    logger.debug(s"Reading entry $index")
    val entry = if (index == 0) {
      Some(defaultLastEntry)
    } else try {
      logger.debug(s"position to seek to: ${indexMap.get(index)}")
      file.seek(indexMap.get(index))
      val entry = LogEntryHelper.read(file)
      Some(entry)
    } catch { case e: Exception => logger.error(s"Could not read at $index", e); None}
    logger.debug(s"Read $entry")
    entry

  }
  var lastEntry : LogEntry = read(indexMap.lastKey()).get
  def append(term: Long, data: AddOrRemove) : Long = {
    append(new LogEntry(indexMap.lastKey() + 1, term, data))
  }
  def append(entry : LogEntry): Long = {
    logger.debug(s"Writing log entry $entry. indexMap is $indexMap, file length ${file.length()}")
    file.seek(file.length)
    lastEntry = entry
    indexMap.put(entry.getIndex, file.length)
    LogEntryHelper.write(lastEntry, file)

    logger.debug(s"Written log entry $lastEntry. indexMap is $indexMap, file length ${file.length()}")
    lastEntry.getIndex
  }
  def rollback(index : Long): Unit = {
    file.setLength(indexMap.get(index))
    val indexes = indexMap.asScala.keys.filter(_ >= index)
    indexes.foreach(indexMap.remove(_))
    lastEntry = read(indexMap.lastKey()).get
  }
  override def close(): Unit = {
    file.close()
  }
}

class State(fileName : String, intialValue : VoteAndTerm) extends AutoCloseable {
  val file : RandomAccessFile = new RandomAccessFile(fileName, "rws")
  private var state : VoteAndTerm = if(file.length() == 0) intialValue else read()
  private def write(entry : VoteAndTerm): Unit = {
    LoggerFactory.getLogger(classOf[State]).info(s"Writing state $entry")
    file.seek(0)
    VoteAndTerm.write(entry, file)
  }
  def set(state : VoteAndTerm) : Unit = {
    this.state = state
    write(state)
  }
  def read() : VoteAndTerm = {
    file.seek(0)
    VoteAndTerm.read(file)
  }
  def get() : VoteAndTerm = {
    state
  }
  override def close(): Unit = {
    file.close()
  }
}

case class VoteAndTerm(vote: Int, term : Long)

object VoteAndTerm {

  def write(t: VoteAndTerm, file: RandomAccessFile): Unit = {
    file.writeInt(t.vote)
    file.writeLong(t.term)
  }

  def read(file: RandomAccessFile): VoteAndTerm = {
    VoteAndTerm(file.readInt(), file.readLong())
  }
}


object LogEntryHelper {

  def write(entry : LogEntry, file : RandomAccessFile) : Unit = {
    val bos = new ByteArrayOutputStream()
    val os = new DataOutputStream(bos)
    os.writeLong(entry.getTerm)
    os.writeLong(entry.getIndex)
    os.writeInt(if(entry.getData.getOp == Operation.Remove) -1 else entry.getData.getValue.remaining())
    val bytes = new Array[Byte](entry.getData.getValue.remaining())
    entry.getData.getValue.get(bytes)
    os.write(bytes)

    file.write(bos.toByteArray)
  }

  def read(file : RandomAccessFile) : LogEntry = {
    val term = file.readLong()
    val index = file.readLong()
    val length = file.readInt()
    val bytes = if (length == -1) ZeroBytes() else { val array = new Array[Byte](length)
      file.readFully(array)
      ByteBuffer.wrap(array)}
    new LogEntry(index, term, new AddOrRemove(if (length == -1) Operation.Remove else Operation.Add, bytes))

  }
}


