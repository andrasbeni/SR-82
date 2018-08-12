package com.github.andrasbeni.sr82

import java.nio.ByteBuffer
import java.util
import java.util.stream.Collectors

import com.github.andrasbeni.sr82.distributedmap._
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by andrasbeni on 11/4/17.
  */

trait StateMachineFactory {
  def create(persistence: Persistence) : StateMachine[_, _]
}

trait StateMachine[C, R] {
  val logger : Logger = LoggerFactory.getLogger(classOf[StateMachine[C, R]])

  def persistence : Persistence

  def applyToIndex(maxIndexToApply : Long) : Map[Long, ByteBuffer] = {
    logger.debug(s"Applying from ${persistence.lastApplied + 1} to $maxIndexToApply")
    (persistence.lastApplied + 1 to maxIndexToApply).map(index =>
      (index, toBuffer(applyCommandAt(index)))).toMap
  }

  private def applyCommandAt(index : Long) : R = {
    logger.debug(s"Now applying index: $index. Current state: $this")
    val entry = persistence.log.read(index).get
    val result = applyCommand(toCommand(entry.getData))
    logger.debug(s"After applying: $this")
    result
  }

  def toCommand(byteBuffer: ByteBuffer) : C

  def applyCommand(command : C) : R

  def toBuffer(result : R) : ByteBuffer

}





