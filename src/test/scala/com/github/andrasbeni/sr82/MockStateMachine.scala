package com.github.andrasbeni.sr82
import java.nio.ByteBuffer

import scala.collection.mutable

/**
  * Created by andrasbeni on 8/12/18.
  */
class MockStateMachine(val persistence: Persistence) extends StateMachine[String, String] {

  private val responses : mutable.Map[String, String] = new mutable.HashMap[String, String]

  def response(command : String, result : String) = responses.put(command, result)

  override def toCommand(byteBuffer: ByteBuffer): String = new String(byteBuffer.array())

  override def applyCommand(command: String): String = responses(command)

  override def toBuffer(result: String): ByteBuffer = ByteBuffer.wrap(result.getBytes())
}
