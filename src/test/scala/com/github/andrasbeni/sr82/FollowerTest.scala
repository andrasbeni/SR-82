package com.github.andrasbeni.sr82

import java.util.{Collections, Properties}
import java.util.concurrent.Future

import com.github.andrasbeni.sr82.raft.AppendEntriesRequest
import org.junit.{Before, Test}
import org.junit.Assert._
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._


class FollowerTest {

  val minTimeout = 1000
  val maxTimeout = 6 * minTimeout / 5

  var follower : Follower = _
  var persistence : Persistence = _
  var stateMachine : StateMachine[_, _] = _
  var cluster : Cluster = _
  var executor : FakeTimeExecutor = _
  var roleListener : Role => Unit = _


  @Before def setup(): Unit = {
    val config = new Properties()
    config.setProperty("election.timeout.min", minTimeout.toString)
    config.setProperty("election.timeout.max", maxTimeout.toString)
    persistence = mock(classOf[Persistence])
    stateMachine = new MockStateMachine(persistence)
    cluster = mock(classOf[Cluster])
    executor = new FakeTimeExecutor
    roleListener = mock(classOf[Role=>Unit])
    follower = new Follower(config, stateMachine, persistence, cluster, executor, roleListener)
  }


  @Test def testStartRole() : Unit = {
    follower.startRole()
    assertEquals(1, executor.workQueue.size)
    verify(roleListener).apply(ArgumentMatchers.eq(follower))
  }

  @Test def testBecomeCandidateWhenNoAppendEntries() : Unit = {
    follower.startRole()
    executor.passTime(maxTimeout + 1)
    verify(roleListener).apply(any(classOf[Candidate]))
  }

  @Test def testAppendEntriesPreventsBecomingCandidate() : Unit = {
    when(persistence.term).thenReturn(2)
    follower.startRole()
    verify(roleListener).apply(ArgumentMatchers.eq(follower))
    executor.passTime(800)
    follower.appendEntries(new AppendEntriesRequest(1L, 1, 1L, 1L, Collections.emptyList(), 0L))
    executor.passTime(800)
    verifyNoMoreInteractions(roleListener)
  }

  @Test def becomeFollower() : Unit = {
    follower.becomeFollower(7)
    verify(cluster).leaderId = ArgumentMatchers.eq(7)
  }

}
