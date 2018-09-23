package com.github.andrasbeni.sr82.raft.example

import java.net.InetSocketAddress

import com.github.andrasbeni.sr82.distributedmap._
import com.github.andrasbeni.sr82.raft._
import org.apache.avro.ipc.NettyTransceiver
import org.apache.avro.ipc.specific.SpecificRequestor
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}



/**
  * Created by andrasbeni on 11/9/17.
  */
class Client(private val hostPorts : List[(String, Int)]) {

  val logger : Logger = LoggerFactory.getLogger(classOf[Client])

  private val avroClients: Map[(String,Int), Raft] = hostPorts.map {
    case ((host, port)) =>
      val client = new NettyTransceiver(new InetSocketAddress(host, port))
      ((host, port), SpecificRequestor.getClient(classOf[Raft], client))
  }.toMap

  private var currentLeader : (String, Int) = avroClients.keySet.toList((math.random()*avroClients.size).toInt)


  def request(request : MapCommand) : Try[MapResult] = {
    var lastException : Option[Exception] = None
    var tries = 0
    while(tries < 10) {
      try {
        val resp = avroClients(currentLeader).changeState(request.toByteBuffer)
        val result = MapResult.fromByteBuffer(resp)
        return Success(result)
      } catch  {
        case e : NotLeader =>
          lastException = Some(e)
          LoggerFactory.getLogger(classOf[Client]).info(s"Leader changed to: ${e.getLeaderAddress}")
          currentLeader = (e.getLeaderAddress.getHost.toString, e.getLeaderAddress.getPort)

        case e : Exception =>
          lastException = Some(e)
          LoggerFactory.getLogger(classOf[Client]).warn("Failed", e)
          currentLeader = hostPorts((math.random()*hostPorts.size).toInt)
      }
      tries += 1
    }
    Failure(lastException.get)
  }

  def put(entry: MapEntry) : MapEntry = {
    val command = new MapCommand(new PutCommand(entry))
    val response = request(command)
    response.get.getValue.asInstanceOf[MapEntry]

  }

  def checkAndPut(entryAndOldValue: EntryAndOldValue) : MapEntry = request(new MapCommand(new CheckAndPutCommand(entryAndOldValue))).get.getValue.asInstanceOf

  def get(key: CharSequence) : Array[Byte]= {
    val buff = request(new MapCommand(new GetCommand(key))).get.getValue.asInstanceOf[MapEntry].getValue
    val array = new Array[Byte](buff.remaining())
    buff.get(array)
    array
  }

  def keys(prefix: CharSequence) : java.util.List[String] = request(new MapCommand(new KeysCommand(prefix))).get.getValue.asInstanceOf


}
