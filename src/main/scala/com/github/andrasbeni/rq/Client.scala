package com.github.andrasbeni.rq

import java.net.InetSocketAddress
import java.nio.ByteBuffer

import com.github.andrasbeni.rq.proto.Raft
import org.apache.avro.ipc.NettyTransceiver
import org.apache.avro.ipc.specific.SpecificRequestor
import org.slf4j.{Logger, LoggerFactory}



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


  def add(value: Array[Byte]): Unit = {
    var tries = 0
    while(tries < 10) {
      try {
        val resp = avroClients(currentLeader).add(ByteBuffer.wrap(value))
        if (resp.getSuccess)
          return
        else {
          do {
            currentLeader = (resp.getLeader.getHost.toString, resp.getLeader.getPort)
          } while (currentLeader._2 == -1 && {Thread.sleep(1000); true})
        }
      } catch  {
        case e : Exception =>
          LoggerFactory.getLogger(classOf[Client]).info("Failed", e)
          currentLeader = hostPorts((math.random()*hostPorts.size).toInt)
      }
      tries += 1
    }
  }
  def next: Array[Byte] = {
    var tries = 0
    while(tries < 10) {
      try {
        logger.debug(s"next with leader $currentLeader")
        val resp = avroClients(currentLeader).next()
        if (resp.getSuccess)
          return resp.getValue.array()
        else {
          do {
            currentLeader = (resp.getLeader.getHost.toString, resp.getLeader.getPort)
          } while (currentLeader._2 == -1 && {Thread.sleep(1000); true})
        }
      } catch  {
        case e : Exception =>
          logger.info("Failed", e)
          currentLeader = hostPorts((hostPorts.indexOf(currentLeader) + 1) % hostPorts.size )
      }
      tries += 1
    }
    throw new RuntimeException

  }
  def remove() : Unit = {
    var tries = 0
    while(tries < 10) {
      try {
        val resp = avroClients(currentLeader).remove()
        if (resp.getSuccess)
          return
        else {
          do {
            currentLeader = (resp.getLeader.getHost.toString, resp.getLeader.getPort)
          } while (currentLeader._2 == -1 && {Thread.sleep(1000); true})
        }
      } catch  {
        case e : Exception =>
          LoggerFactory.getLogger(classOf[Client]).info("Failed", e)
          currentLeader = hostPorts((math.random()*hostPorts.size).toInt)
      }
      tries += 1
    }

  }
}
