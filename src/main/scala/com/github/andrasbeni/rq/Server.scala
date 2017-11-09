package com.github.andrasbeni.rq

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent._

import com.github.andrasbeni.rq.proto._
import org.apache.avro.ipc.NettyServer
import org.apache.avro.ipc.specific.SpecificResponder
import org.slf4j.{Logger, LoggerFactory, MDC}


object ZeroBytes {
  private val emptyBuffer =  ByteBuffer.wrap(Array.emptyByteArray)
  def apply() : ByteBuffer = emptyBuffer
}

class Executor extends AutoCloseable {
  val logger : Logger = LoggerFactory.getLogger( classOf[Executor] )

  private val executor : ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
  def submit[T](callable : () => T): Future[T] =
    executor.submit(() =>
      try{callable.apply()}
      catch {
        case e : Exception => logger.error("Error", e); throw e
      })
  def schedule[T](callable : () => T, milliseconds : Long): ScheduledFuture[T] =
    executor.schedule(() =>
      try{callable.apply()}
      catch{
        case e : Exception => logger.error("Error", e); throw e
      }, milliseconds, TimeUnit.MILLISECONDS)
  override def close(): Unit = executor.shutdownNow()

}

class Listener(executor : Executor, cluster : Cluster) extends AutoCloseable {
  var role : Role = _
  var server = new NettyServer(new SpecificResponder(classOf[Raft], new Raft {
    override def appendEntries(req: AppendEntriesReq) : AppendEntriesResp = executor.submit(() => role.appendEntries(req)).get
    override def requestVote(req: VoteReq) : VoteResp = executor.submit(() => role.requestVote(req)).get

    override def add(value: ByteBuffer) : AddOrRemoveResp = executor.submit(()=> role.add(value)).get.get
    override def remove() : AddOrRemoveResp = executor.submit(()=> role.remove()).get.get
    override def next() : NextResp = executor.submit(() => role.next()).get

  }), new InetSocketAddress(cluster.localHostPort._1, cluster.localHostPort._2))
  LoggerFactory.getLogger(classOf[Listener]).info(s"Listener starting on ${cluster.localHostPort}")

  override def close() : Unit = {
    LoggerFactory.getLogger(classOf[Listener]).info(s"Listener closing on ${cluster.localHostPort}")
    server.close()
  }
}


class Server(val config : Properties) {
  val logger : Logger = LoggerFactory.getLogger( classOf[Server] )
  val executor = new Executor()
  val cluster = new Cluster(config, executor)
  val persistence = new Persistence(config)()
  val stateMachine = new StateMachine(persistence)
  val listener = new Listener(executor, cluster)

  def close(): Unit = {
    executor.submit(()=>{
      logger.info(s"Server ${cluster.localId} shutting down")
      listener.close()
      executor.close()
      persistence.close()
    }).get()
  }

  def start() : Unit = {

    executor.submit(()=> {
      MDC.put("server", cluster.localId.toString)
      new Follower(config, stateMachine, persistence, cluster, executor, role => listener.role = role).startRole()
    })
  }

 }
