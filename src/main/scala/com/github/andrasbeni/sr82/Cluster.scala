package com.github.andrasbeni.sr82

import java.util.Properties

import com.github.andrasbeni.sr82.raft._

import scala.collection.JavaConverters._
import org.apache.avro.ipc.{Callback, NettyTransceiver}
import org.apache.avro.ipc.specific.SpecificRequestor
import java.net.InetSocketAddress
import java.util
import java.util.concurrent.{ExecutorService, Executors}

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.Set

class Hollaback[T](val warningOnError : String, val successHandler : T => Unit)

class RPC(proxy : Raft.Callback, executor : Executor, backgroundThreads : ExecutorService) {
  val logger : Logger = LoggerFactory.getLogger(classOf[RPC])
  def appendEntries(req: AppendEntriesRequest, callback: Hollaback[AppendEntriesResponse]) : Unit = {
    backgroundThreads.submit(() => {
      proxy.appendEntries(req, new Callback[AppendEntriesResponse] {
        override def handleResult(result: AppendEntriesResponse): Unit = {
          executor.submit(() => callback.successHandler(result))
        }

        override def handleError(error: Throwable): Unit = {
          logger.warn(callback.warningOnError, error)
        }
      })
    }, null)
  }

  def requestVote(req: VoteRequest, callback: Hollaback[VoteResponse]) : Unit = {
    backgroundThreads.submit(() => {
      proxy.requestVote(req, new Callback[VoteResponse] {
        override def handleResult(result: VoteResponse): Unit = {
          executor.submit(() => callback.successHandler(result))
        }

        override def handleError(error: Throwable): Unit = {
          logger.warn(callback.warningOnError, error)
        }
      })
    }, null)
  }

}


class Cluster(config : Properties, val executor : Executor) {

  private val hostPorts: collection.Map[Int, (String, Int)] = config.asScala.
    filterKeys(key => key.startsWith("server.") && "server.id" != key).
    map(e => (e._1.substring("server.".length).toInt, e._2)).
    map(e => (e._1, {val array = e._2.split(':'); (array(0), array(1).toInt) }))

  private val clients = new util.HashMap[Int, RPC]

  val localId = config.getProperty("server.id").toInt

  val size : Int = hostPorts.size

  def serverIds : Set[Int] = hostPorts.keySet.filter(_ != localId)

  var leaderId : Int = -1

  val localHostPort : (String, Int) = hostPorts(localId)

  private val backgroundThreads = Executors.newCachedThreadPool()

  def clientTo(node : Int): RPC =
    if (clients.containsKey(node))
      clients.get(node)
    else {
      val hostPort : (String, Int) = hostPorts(node)
      val client = new NettyTransceiver(new InetSocketAddress(hostPort._1, hostPort._2))
      val proxy: Raft.Callback = SpecificRequestor.getClient(classOf[Raft.Callback], client)
      val rpc = new RPC(proxy, executor, backgroundThreads)
      clients.put(node, rpc)
      rpc
    }

  def currentLeader : LeaderAddress = {
    val leaderHostPort = hostPorts.getOrElse(leaderId, ("", -1))
    new LeaderAddress(leaderHostPort._1, leaderHostPort._2)
  }

}
