package com.github.andrasbeni.sr82

import java.nio.ByteBuffer
import java.util.Collections
import java.util.concurrent.{CompletableFuture, Executors}

import com.github.andrasbeni.sr82.raft._
import org.apache.avro.ipc.Callback
import org.junit.Assert._
import org.junit.{Before, Test}
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock

class RPCTest {

  val appendEntriesReqSuccess = new AppendEntriesRequest(17L, 23, 29L, 31L, Collections.emptyList[LogEntry](), 37L)
  val appendEntriesReqFailure = new AppendEntriesRequest(19L, 23, 29L, 31L, Collections.emptyList[LogEntry](), 37L)
  val appenEntriesResp = new AppendEntriesResponse(17L, true)
  val voteReqSuccess = new VoteRequest(17L, 23, 29L, 31L)
  val voteReqFailure = new VoteRequest(19L, 23, 29L, 31L)
  val voteResp = new VoteResponse(17L, true)
  var rpc : RPC = _
  var mockProxy : Raft.Callback = _
  var mockExecutor : Executor = _
  var inExecutor : Boolean = _


  @Before def setup(): Unit = {

    class MockProxy extends Raft.Callback {

      override def appendEntries(req: AppendEntriesRequest, callback: Callback[AppendEntriesResponse]): Unit =
        if (appendEntriesReqSuccess.equals(req)) {
          inExecutor = true
          callback.handleResult(appenEntriesResp)
          inExecutor = false
        } else {
          callback.handleError(new RuntimeException)
        }

      override def requestVote(req: VoteRequest, callback: Callback[VoteResponse]): Unit =
        if ( voteReqSuccess.equals(req) ) {
          inExecutor = true
          callback.handleResult(voteResp)
          inExecutor = false
        } else {
          callback.handleError(new RuntimeException)
        }
      override def changeState(req: ByteBuffer, callback: Callback[ByteBuffer]): Unit = {
        //        if ( new String(stateChangeReqSuccess.array()).equals(new String(req.array())) ) {
        //          inExecutor = true
        //          callback.handleResult(stateChangeResp)
        //          inExecutor = false
        //        } else {
        //          callback.handleError(new RuntimeException)
        //        }
      }

      override def appendEntries(req: AppendEntriesRequest): AppendEntriesResponse = ???
      override def requestVote(req: VoteRequest): VoteResponse = ???
      override def changeState(req: ByteBuffer): ByteBuffer = ???


    }

    inExecutor = false
    mockProxy = spy(new MockProxy)
    mockExecutor = mock(classOf[Executor])
    doAnswer((invocationOnMock: InvocationOnMock) => {
      val value = invocationOnMock.getArgument(0).asInstanceOf[() => _].apply()
      CompletableFuture.completedFuture(value)
    }).when(mockExecutor).submit(any())
    rpc = new RPC(mockProxy, mockExecutor, Executors.newCachedThreadPool())
  }

  @Test def testAppendEntries(): Unit = {
    val hollaback : Hollaback[AppendEntriesResponse]= new Hollaback("Should not happen", resp => {
      assertEquals(appenEntriesResp, resp)
      assertTrue(inExecutor)
    })
    rpc.appendEntries(
      appendEntriesReqSuccess,
      hollaback)
    Thread.sleep(1000)
    verify(mockProxy).appendEntries(ArgumentMatchers.eq(appendEntriesReqSuccess), any())
    verify(mockExecutor).submit[AppendEntriesResponse](any())
    verifyNoMoreInteractions(mockExecutor)
    verifyNoMoreInteractions(mockProxy)
  }

  @Test def testAppendEntriesThrows(): Unit = {
    val hollaback : Hollaback[AppendEntriesResponse]= new Hollaback("Should  happen", resp => {
      fail("Should not call success handler on failure")
    })
    rpc.appendEntries(
      appendEntriesReqFailure,
      hollaback)
    Thread.sleep(1000)
    verify(mockProxy).appendEntries(ArgumentMatchers.eq(appendEntriesReqFailure), any())
    verifyNoMoreInteractions(mockExecutor)
    verifyNoMoreInteractions(mockProxy)
  }

  @Test def testRequestVote(): Unit = {
    val hollaback : Hollaback[VoteResponse]= new Hollaback("Should not happen", resp => {
      assertEquals(voteResp, resp)
      assertTrue(inExecutor)
    })
    rpc.requestVote(
      voteReqSuccess,
      hollaback)
    Thread.sleep(1000)
    verify(mockProxy).requestVote(ArgumentMatchers.eq(voteReqSuccess), any())
    verify(mockExecutor).submit[AppendEntriesResponse](any())
    verifyNoMoreInteractions(mockExecutor)
    verifyNoMoreInteractions(mockProxy)
  }

  @Test def testRequestVoteThrows(): Unit = {
    val hollaback : Hollaback[VoteResponse]= new Hollaback("Should  happen", resp => {
      fail("Should not call success handler on failure")
    })
    rpc.requestVote(
      voteReqFailure,
      hollaback)
    Thread.sleep(1000)
    verify(mockProxy).requestVote(ArgumentMatchers.eq(voteReqFailure), any())
    verifyNoMoreInteractions(mockExecutor)
    verifyNoMoreInteractions(mockProxy)
  }


}
