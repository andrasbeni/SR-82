package com.github.andrasbeni.sr82

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent._

import com.github.andrasbeni.sr82.map.MapStateMachine
import com.github.andrasbeni.sr82.raft._
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
  private val logger = LoggerFactory.getLogger(classOf[Listener])
  var role : Role = _
  var server = new NettyServer(new SpecificResponder(classOf[Raft], new Raft {
    override def appendEntries(req: AppendEntriesRequest) : AppendEntriesResponse = executor.submit(() => role.appendEntries(req)).get
    override def requestVote(req: VoteRequest) : VoteResponse = executor.submit(() => role.requestVote(req)).get

    override def changeState(req: ByteBuffer) : ByteBuffer = try {
      val result = executor.submit(() => role.changeState(req)).get.get
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
  val executor = new Executor()
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
