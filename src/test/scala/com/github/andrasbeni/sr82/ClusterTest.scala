package com.github.andrasbeni.sr82

import java.net.{InetSocketAddress, ServerSocket}
import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.{CompletableFuture, CountDownLatch, TimeUnit}

import com.github.andrasbeni.sr82.raft._
import org.apache.avro.ipc.NettyServer
import org.apache.avro.ipc.specific.SpecificResponder
import org.junit.{Before, Test}
import org.junit.Assert._
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.Mockito.{doAnswer, mock}
import org.mockito.invocation.InvocationOnMock


class ClusterTest {

  var cluster : Cluster = _
  var mockExecutor : Executor = _

  private def freePort() : Int = {
    var socket : ServerSocket = null
    try {
      socket = new ServerSocket(0)
      socket.getLocalPort
    } finally {
      if (socket != null) socket.close()
    }
  }
  val port1: Int = freePort()
  val port2: Int = freePort()
  val port3: Int = freePort()

  @Before def setup() : Unit = {
    val config : Properties = new Properties()
    config.setProperty("server.1", s"localhost:$port1")
    config.setProperty("server.2", s"localhost:$port2")
    config.setProperty("server.3", s"localhost:$port3")
    config.setProperty("server.id", "2")
    mockExecutor = mock(classOf[Executor])
    doAnswer((invocationOnMock: InvocationOnMock) => {
      val value = invocationOnMock.getArgument(0).asInstanceOf[() => _].apply()
      CompletableFuture.completedFuture(value)
    }).when(mockExecutor).submit(any())

    cluster = new Cluster(config, mockExecutor)
  }

  @Test def testLocalId() : Unit = {
    assertEquals(2, cluster.localId)
  }

  @Test def testLocalHostPort() : Unit = {
    assertEquals(("localhost", port2), cluster.localHostPort)
  }

  @Test def testSize() : Unit = {
    assertEquals(3, cluster.size)
  }

  @Test def testServerIds() : Unit = {
    assertEquals(Set(1,3), cluster.serverIds)
  }

  @Test def testLeader() : Unit = {
    cluster.leaderId = 1
    assertEquals(new LeaderAddress("localhost", port1), cluster.currentLeader)
  }

  @Test def testClientTo() : Unit = {
    val voteResp : VoteResponse = new VoteResponse(17L, true)
    val voteReq : VoteRequest = new VoteRequest(3L, 2, 5L, 7L)
    var correctRequestSent = false
    val conversationFinished = new CountDownLatch(1)

    var server = new NettyServer(new SpecificResponder(classOf[Raft], new Raft {
      override def appendEntries(req: AppendEntriesRequest) : AppendEntriesResponse = ???
      override def requestVote(req: VoteRequest) : VoteResponse = {
        correctRequestSent = req.equals(voteReq)
        voteResp
      }
      override def changeState(req: ByteBuffer): ByteBuffer = ???

    }), new InetSocketAddress("localhost", port3))
    server.start()
    Thread.sleep(1000)

    val rpc : RPC = Mockito.spy(cluster.clientTo(3))
    rpc.requestVote(voteReq, new Hollaback[VoteResponse]("Should not fail", resp => {
      assertEquals(voteResp, resp)
      conversationFinished.countDown()
    }))
    conversationFinished.await(2000, TimeUnit.MILLISECONDS)
    assertTrue(correctRequestSent)

    server.close()
  }




}
