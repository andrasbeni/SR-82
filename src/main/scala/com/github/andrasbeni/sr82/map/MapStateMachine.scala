package com.github.andrasbeni.sr82.map

import java.nio.ByteBuffer
import java.util
import java.util.stream.Collectors

import com.github.andrasbeni.sr82.{Persistence, StateMachine, StateMachineFactory}
import com.github.andrasbeni.sr82.distributedmap._

class MapStateMachineFactory extends StateMachineFactory {
  override def create(persistence: Persistence): StateMachine[_, _] = new MapStateMachine(persistence)
}

class MapStateMachine(val persistence : Persistence) extends StateMachine[MapCommand, MapResult] {

  private val map : java.util.Map[String, Array[Byte]] = new util.HashMap[String, Array[Byte]]()

  private def read(buff : ByteBuffer) : Array[Byte] = {
    val array = new Array[Byte](buff.remaining())
    buff.get(array)
    array
  }

  private def equal(buff : ByteBuffer, array : Array[Byte]) : Boolean = {
    (array.length == buff.remaining()) &&
      array.indices.forall( i => {array(i) == buff.get(buff.position() + i)})
  }

  /**
    */
  def put(entry: MapEntry): Void = {
    map.put(entry.getKey.toString, read(entry.getValue))
    null
  }

  /**
    */
  def get(key: CharSequence): MapEntry = new MapEntry(key, ByteBuffer.wrap(map.get(key.toString)))

  /**
    */
  def checkAndPut(entryAndOldValue: EntryAndOldValue): MapEntry = {
    val key = entryAndOldValue.getNewEntry.getKey.toString
    val newValue = entryAndOldValue.getNewEntry.getValue
    if (equal(entryAndOldValue.getOldValue, map.get(key))) {
      map.put(key, read(newValue))
      new MapEntry(key, newValue)
    } else {
      new MapEntry(key, ByteBuffer.wrap(map.get(key)))
    }
  }


  /**
    */
  def keys(prefix: CharSequence): util.List[CharSequence] = map.keySet().stream().
    filter(_.startsWith(prefix.toString)).collect(Collectors.toList[CharSequence])



  override def toCommand(byteBuffer: ByteBuffer): MapCommand = MapCommand.fromByteBuffer(byteBuffer)



  override def applyCommand(command: MapCommand): MapResult = {

    command.getValue match {
      case p : PutCommand => new MapResult(put(p.getEntry))
      case g : GetCommand => new MapResult(get(g.getKey))
      case c : CheckAndPutCommand => new MapResult(checkAndPut(c.getEntryAndOldValue))
      case k : KeysCommand => new MapResult(keys(k.getPrefix))
    }

  }

  override def toBuffer(result: MapResult): ByteBuffer = result.toByteBuffer

  override def toString: String = getClass.getSimpleName + ": " + map
}
