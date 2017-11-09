package com.github.andrasbeni.rq
import java.net.{InetSocketAddress, ServerSocket}
import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.{CompletableFuture, CountDownLatch, Future}

import com.github.andrasbeni.rq.proto._
import org.apache.avro.ipc.{Callback, NettyTransceiver}
import org.apache.avro.ipc.specific.SpecificRequestor
import org.junit.Assert._
import org.junit.{Before, Test}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{doAnswer, mock, when}
import org.mockito.invocation.InvocationOnMock


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
      val value = invocationOnMock.getArgument(0).asInstanceOf[() => _].apply()
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
    val voteResp : VoteResp = new VoteResp(17L, true)
    val voteReq : VoteReq = new VoteReq(3L, 2, 5L, 7L)
    var response : VoteResp = null
    val latch = new CountDownLatch(1)
    when(role.requestVote(voteReq)).thenReturn(voteResp)
    proxy.requestVote(voteReq, new Callback[VoteResp]() {
      override def handleResult(result: VoteResp): Unit = {
        response = result
        latch.countDown()
      }

      override def handleError(error: Throwable): Unit = ???
    })
    latch.await()
    assertEquals(voteResp, response)
  }

  @Test def testAdd(): Unit = {
    val addReq = ByteBuffer.wrap("Hello".getBytes)
    val addResp = new AddOrRemoveResp(new LeaderAddress("someServer", 11111), false)
    var response : AddOrRemoveResp = null
    val latch = new CountDownLatch(1)
    when(role.add(addReq)).thenReturn(CompletableFuture.completedFuture(addResp))
    proxy.add(addReq, new Callback[AddOrRemoveResp]() {
      override def handleResult(result: AddOrRemoveResp): Unit = {
        response = result
        latch.countDown()
      }

      override def handleError(error: Throwable): Unit = ???
    })
    latch.await()
    assertEquals(addResp, response)
  }

  @Test def testRemove(): Unit = {
    val removeResp = new AddOrRemoveResp(new LeaderAddress("someServer", 11111), false)
    var response : AddOrRemoveResp = null
    val latch = new CountDownLatch(1)
    when(role.remove()).thenReturn(CompletableFuture.completedFuture(removeResp))
    proxy.remove(new Callback[AddOrRemoveResp]() {
      override def handleResult(result: AddOrRemoveResp): Unit = {
        response = result
        latch.countDown()
      }

      override def handleError(error: Throwable): Unit = ???
    })
    latch.await()
    assertEquals(removeResp, response)
  }

}
