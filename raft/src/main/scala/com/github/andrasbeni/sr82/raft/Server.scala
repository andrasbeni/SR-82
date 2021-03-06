package com.github.andrasbeni.sr82.raft

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent._

import org.apache.avro.ipc.NettyServer
import org.apache.avro.ipc.specific.SpecificResponder
import org.slf4j.{Logger, LoggerFactory, MDC}


object ZeroBytes {
  private val emptyBuffer =  ByteBuffer.wrap(Array.emptyByteArray)
  def apply() : ByteBuffer = emptyBuffer
}

class Listener(executor : Executor, cluster : Cluster) extends AutoCloseable {
  private val logger = LoggerFactory.getLogger(classOf[Listener])
  var role : Role = _
  var server = new NettyServer(new SpecificResponder(classOf[Raft], new Raft {
    override def appendEntries(req: AppendEntriesRequest) : AppendEntriesResponse = executor.submit(() => role.appendEntries(req)).get
    override def requestVote(req: VoteRequest) : VoteResponse = executor.submit(() => role.requestVote(req)).get

    override def changeState(req: ByteBuffer) : ByteBuffer = try {
      val submitted: Future[Future[ByteBuffer]] =  executor.submit(() => role.changeState(req))
      val resultOfSubmit = submitted.get()
      val result = resultOfSubmit.get()
      result
    } catch {
      case e : Exception if e.isInstanceOf[ExecutionException] => throw e.getCause
      case e : Exception => throw e
    }

  }), new InetSocketAddress(cluster.localHostPort._1, cluster.localHostPort._2))
  logger.info(s"Listener starting on ${cluster.localHostPort}")

  override def close() : Unit = {
    logger.info(s"Listener closing on ${cluster.localHostPort}")
    server.close()
  }
}


class Server(val config : Properties) {
  val logger : Logger = LoggerFactory.getLogger( classOf[Server] )
  val executor = new DefaultExecutor()
  val cluster = new Cluster(config, executor)
  val persistence = new Persistence(config)()
  val stateMachine : StateMachine[_, _] = createStateMachine
  val listener = new Listener(executor, cluster)

  def close(): Unit = {
    executor.submit(()=>{
      logger.info(s"Server ${cluster.localId} shutting down")
      listener.close()
      executor.close()
      persistence.close()
    }).get()
  }

  def createStateMachine : StateMachine[_, _] = {
    val className = config.getProperty("state.machine.factory")
    val factoryClass = Class.forName(className)
    val factory = factoryClass.newInstance().asInstanceOf[StateMachineFactory]
    val stateMachine = factory.create(persistence)
    stateMachine

  }

  def start() : Unit = {

    executor.submit(()=> {
      MDC.put("server", cluster.localId.toString)
      new Follower(config, stateMachine, persistence, cluster, executor, listener.role = _).startRole()
    })
  }

 }
