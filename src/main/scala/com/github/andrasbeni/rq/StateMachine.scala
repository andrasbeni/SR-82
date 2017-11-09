package com.github.andrasbeni.rq

import java.nio.ByteBuffer
import java.util

import com.github.andrasbeni.rq.proto.Operation
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by andrasbeni on 11/4/17.
  */
class StateMachine(val persistence : Persistence) {

  val logger : Logger = LoggerFactory.getLogger(classOf[StateMachine])

  private val indexes = new util.LinkedList[Long]()

  private def applyCommandAt(index : Long) = {
    logger.debug(s"Now applying index: $index. Indexes: $indexes")
    val entry = persistence.log.read(index).get
    entry.getData.getOp match {
      case Operation.Add => indexes.add(index)
      case Operation.Remove => indexes.remove(0)
    }
    logger.debug(s"after applying: $indexes")
  }
  def applyToIndex(maxIndexToApply : Long) : Unit = {
    logger.debug(s"applying from ${persistence.lastApplied + 1} to $maxIndexToApply")
    for (index <- persistence.lastApplied + 1 to maxIndexToApply) {
      applyCommandAt(index)
    }
  }

  def next() : Option[ByteBuffer] = {
    logger.debug(s"next called when $indexes")
    if (indexes.size() > 0) persistence.log.read(indexes.get(0)).map(_.getData.getValue)
    else None
  }


}
