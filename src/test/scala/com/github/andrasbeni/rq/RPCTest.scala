package com.github.andrasbeni.rq

import java.nio.ByteBuffer
import java.util.Collections
import java.util.concurrent.{CompletableFuture, Executors}

import com.github.andrasbeni.rq.proto.{Raft, _}
import org.apache.avro.ipc.Callback
import org.junit.Assert._
import org.junit.{Before, Test}
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock

class RPCTest {

  val appendEntriesReqSuccess = new AppendEntriesReq(17L, 23, 29L, 31L, Collections.emptyList[LogEntry](), 37L)
  val appendEntriesReqFailure = new AppendEntriesReq(19L, 23, 29L, 31L, Collections.emptyList[LogEntry](), 37L)
  val appenEntriesResp = new AppendEntriesResp(17L, true)
  val voteReqSuccess = new VoteReq(17L, 23, 29L, 31L)
  val voteReqFailure = new VoteReq(19L, 23, 29L, 31L)
  val voteResp = new VoteResp(17L, true)
  var rpc : RPC = _
  var mockProxy : Raft.Callback = _
  var mockExecutor : Executor = _
  var inExecutor : Boolean = _


  @Before def setup(): Unit = {

    class MockProxy extends Raft.Callback {

      override def appendEntries(req: AppendEntriesReq, callback: Callback[AppendEntriesResp]): Unit =
        if (appendEntriesReqSuccess.equals(req)) {
          inExecutor = true
          callback.handleResult(appenEntriesResp)
          inExecutor = false
        } else {
          callback.handleError(new RuntimeException)
        }

      override def requestVote(req: VoteReq, callback: Callback[VoteResp]): Unit =
        if ( voteReqSuccess.equals(req) ) {
          inExecutor = true
          callback.handleResult(voteResp)
          inExecutor = false
        } else {
          callback.handleError(new RuntimeException)
        }
      override def add(value: ByteBuffer, callback: Callback[AddOrRemoveResp]): Unit = ???
      override def next(callback: Callback[NextResp]): Unit = ???
      override def remove(callback: Callback[AddOrRemoveResp]): Unit = ???
      override def appendEntries(req: AppendEntriesReq): AppendEntriesResp = ???
      override def requestVote(req: VoteReq): VoteResp = ???
      override def add(value: ByteBuffer): AddOrRemoveResp = ???
      override def next(): NextResp = ???
      override def remove(): AddOrRemoveResp = ???
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
    val hollaback : Hollaback[AppendEntriesResp]= new Hollaback("Should not happen", resp => {
      assertEquals(appenEntriesResp, resp)
      assertTrue(inExecutor)
    })
    rpc.appendEntries(
      appendEntriesReqSuccess,
      hollaback)
    Thread.sleep(1000)
    verify(mockProxy).appendEntries(ArgumentMatchers.eq(appendEntriesReqSuccess), any())
    verify(mockExecutor).submit[AppendEntriesResp](any())
    verifyNoMoreInteractions(mockExecutor)
    verifyNoMoreInteractions(mockProxy)
  }

  @Test def testAppendEntriesThrows(): Unit = {
    val hollaback : Hollaback[AppendEntriesResp]= new Hollaback("Should  happen", resp => {
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
    val hollaback : Hollaback[VoteResp]= new Hollaback("Should not happen", resp => {
      assertEquals(voteResp, resp)
      assertTrue(inExecutor)
    })
    rpc.requestVote(
      voteReqSuccess,
      hollaback)
    Thread.sleep(1000)
    verify(mockProxy).requestVote(ArgumentMatchers.eq(voteReqSuccess), any())
    verify(mockExecutor).submit[AppendEntriesResp](any())
    verifyNoMoreInteractions(mockExecutor)
    verifyNoMoreInteractions(mockProxy)
  }

  @Test def testRequestVoteThrows(): Unit = {
    val hollaback : Hollaback[VoteResp]= new Hollaback("Should  happen", resp => {
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
