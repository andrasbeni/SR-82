package com.github.andrasbeni.sr82

import java.util.Properties
import java.util.concurrent.{CompletableFuture, Future, ScheduledFuture}

import org.junit.{Before, Test}
import org.junit.Assert._
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock


class FollowerTest {

  val minTimeout = 1000
  val maxTimeout = 2000

  var follower : Follower = _
  var persistence : Persistence =_
  var stateMachine : StateMachine[_, _] = _
  var cluster : Cluster = _
  var executor : Executor = _
  var timer : Future[_] = _
  var roleListener : Role => Unit = _


  @Before def setup(): Unit = {
    val config = new Properties()
    config.setProperty("election.timeout.min", minTimeout.toString)
    config.setProperty("election.timeout.max", maxTimeout.toString)
    persistence = mock(classOf[Persistence])
    stateMachine = new MockStateMachine(persistence)
    cluster = mock(classOf[Cluster])
    executor = mock(classOf[Executor])
    doAnswer((invocationOnMock: InvocationOnMock) => {
      val value = invocationOnMock.getArgument(0).asInstanceOf[() => _].apply()
      CompletableFuture.completedFuture(value)
    }).when(executor).submit(any())
    doAnswer((invocationOnMock: InvocationOnMock) => {
      timer = mock(classOf[ScheduledFuture[Unit]])
      timer
    }).when(executor).schedule(any(), any())
    roleListener = mock(classOf[Role=>Unit])
    follower = new Follower(config, stateMachine, persistence, cluster, executor, roleListener)
  }


  @Test def testStartRole() : Unit = {
    follower.startRole()
    verify(executor).schedule(any(), any())
    verify(roleListener).apply(ArgumentMatchers.eq(follower))
  }
}
