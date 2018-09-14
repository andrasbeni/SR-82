package com.github.andrasbeni.sr82
import java.net.{InetSocketAddress, ServerSocket}
import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.{Callable, CompletableFuture, CountDownLatch}

import com.github.andrasbeni.sr82.raft._
import org.apache.avro.ipc.{Callback, NettyTransceiver}
import org.apache.avro.ipc.specific.SpecificRequestor
import org.junit.Assert._
import org.junit.{Before, Ignore, Test}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{doAnswer, mock, when}
import org.mockito.invocation.InvocationOnMock
import org.slf4j.{Logger, LoggerFactory}


class ListenerTest {

  private def freePort() : Int = {
    var socket : ServerSocket = null
    try {
      socket = new ServerSocket(0)
      socket.getLocalPort
    } finally {
      if (socket != null) socket.close()
    }
  }


  var role : Role = _
  var listener : Listener = _
  var mockExecutor : Executor = _
  var proxy : Raft.Callback = _
  val port: Int = freePort()

  @Before def setup() : Unit = {
    role = mock(classOf[Role])
    mockExecutor = mock(classOf[Executor])
    doAnswer((invocationOnMock: InvocationOnMock) => {
      val value = invocationOnMock.getArgument(0).asInstanceOf[Callable[_]].call()
      CompletableFuture.completedFuture(value)
    }).when(mockExecutor).submit(any())
    val config = new Properties()
    config.setProperty("server.id", "1")
    config.setProperty("server.1", s"localhost:$port")
    listener = new Listener(mockExecutor, new Cluster(config, mockExecutor))
    listener.role = role
    val client = new NettyTransceiver(new InetSocketAddress("localhost", port))
    proxy = SpecificRequestor.getClient(classOf[Raft.Callback], client)

  }

  @Test def testRequestVote(): Unit = {
    val voteResp : VoteResponse = new VoteResponse(17L, true)
    val voteReq : VoteRequest = new VoteRequest(3L, 2, 5L, 7L)
    var response : VoteResponse = null
    val latch = new CountDownLatch(1)
    when(role.requestVote(voteReq)).thenReturn(voteResp)
    proxy.requestVote(voteReq, new Callback[VoteResponse]() {
      override def handleResult(result: VoteResponse): Unit = {
        response = result
        latch.countDown()
      }

      override def handleError(error: Throwable): Unit = ???
    })
    latch.await()
    assertEquals(voteResp, response)
  }

  @Test def testChangeState(): Unit = {
    val req = ByteBuffer.wrap("Hello".getBytes)
    val addResp = ByteBuffer.wrap("Hi".getBytes)
    var response : ByteBuffer = null
    val latch = new CountDownLatch(1)
    when(role.changeState(req)).thenAnswer(_ => {
      CompletableFuture.completedFuture(addResp)
    })
    proxy.changeState(req, new Callback[ByteBuffer]() {
      override def handleResult(result: ByteBuffer): Unit = {
        response = result
        latch.countDown()
      }

      override def handleError(error: Throwable): Unit = {
        LoggerFactory.getLogger(classOf[ListenerTest]).info("Error", error)
        latch.countDown()
      }
    })
    latch.await()
    assertEquals("Hi", new String(response.array()))
  }

  @Test def testChangeStateThrows(): Unit = {
    val req = ByteBuffer.wrap("Hello".getBytes)
    val latch = new CountDownLatch(1)
    var actualError : Throwable = null
    val error = NotLeader.newBuilder().setLeaderAddress(new LeaderAddress("someServer", 11111)).build()
    val result = new CompletableFuture[ByteBuffer]()
    result.completeExceptionally(error)
    when(role.changeState(req)).thenReturn(result)
    proxy.changeState(req, new Callback[ByteBuffer]() {
      override def handleResult(result: ByteBuffer): Unit = ???

      override def handleError(error: Throwable): Unit = {
        actualError = error
        latch.countDown()
      }
    })
    latch.await()
    assertEquals(error, actualError)
  }

}
