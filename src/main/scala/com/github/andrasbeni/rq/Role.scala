package com.github.andrasbeni.rq

import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.CompletableFuture.completedFuture
import java.util.concurrent.{Future, ScheduledFuture}

import com.github.andrasbeni.rq.proto._
import org.slf4j.{Logger, LoggerFactory, MDC}

trait RoleFactory {
  def apply(config : Properties, stateMachine : StateMachine, persistence : Persistence, cluster : Cluster, executor : Executor, roleListener : Role => Unit) : Role
}

/**
  * Created by andrasbeni on 11/10/17.
  */
abstract class Role(val config : Properties, val stateMachine : StateMachine, val persistence : Persistence, val cluster : Cluster, val executor : Executor, val roleListener : Role => Unit) {

  val logger : Logger = LoggerFactory.getLogger(getClass)

  def startRole() : Unit = {
    MDC.put("role", getClass.getSimpleName)
    logger.info(s"Changing to role ${getClass.getSimpleName}")
    roleListener(this)
  }

  def stopRole() : Unit = {
    logger.debug(s"Stopping role ${getClass.getSimpleName}")
    alive = false
    cancelTimer()
  }

  def beforeRequestVote() : Unit = {}

  def afterRequestVote() : Unit = {}


  def requestVote(req: VoteReq): VoteResp = {
    logger.debug(s"Received vote request from ${req.getCandidateId} : $req")
    beforeRequestVote()
    val oldVoteAndTerm = persistence.voteAndTerm
    val oldTerm = oldVoteAndTerm.term
    val oldVote = oldVoteAndTerm.vote
    val granted = (req.getTerm > oldTerm || (req.getTerm == oldTerm && (oldVote == -1 || oldVote == req.getCandidateId))) && candidateLogMoreUpToDate(req)
    if (granted) {
        persistence.setVoteAndTerm(req.getCandidateId, req.getTerm)
        becomeFollower(req.getCandidateId)
    }
    afterRequestVote()
    logger.debug(s"Responding to vote request from ${req.getCandidateId}: ${new VoteResp(oldTerm, granted)}")
    new VoteResp(oldTerm, granted)
  }

  def appendEntries(req: AppendEntriesReq) : AppendEntriesResp

  private def candidateLogMoreUpToDate(req: VoteReq): Boolean = {
    val lastEntry = persistence.log.lastEntry
    (req.getLastLogTerm > lastEntry.getTerm) ||
      (req.getLastLogTerm == lastEntry.getTerm &&
        req.getLastLogIndex >= lastEntry.getIndex)
  }

  def becomeFollower(leaderId : Int): Role = {
    cluster.leaderId = leaderId
    convertTo(Follower)
  }

  def remove() : Future[AddOrRemoveResp] =
    completedFuture[AddOrRemoveResp](new AddOrRemoveResp(cluster.currentLeader, false))

  def next() : NextResp =
    new NextResp(cluster.currentLeader, false, ZeroBytes())

  def add(value: ByteBuffer) : Future[AddOrRemoveResp]  =
    completedFuture[AddOrRemoveResp](new AddOrRemoveResp(cluster.currentLeader, false))


  private var timer : Option[ScheduledFuture[_]]= None

  private val minElectionTimeout = config.getProperty("election.timeout.min").toLong

  private val deltaElectionTimeout = config.getProperty("election.timeout.max").toLong - minElectionTimeout

  var alive = true

  def onTimeout() : Unit = convertTo(Candidate)

  def timeout : Long = {
    minElectionTimeout + (Math.random() * deltaElectionTimeout).toLong
  }

  def startTimer() : Unit = {
    timer = Some(executor.schedule(() => onTimeout(), timeout))
  }

  def cancelTimer() : Unit = {
    timer.forall(_.cancel(false))
  }

  def convertTo(newRole : RoleFactory) : Role = {
    stopRole()
    val role = newRole(config, stateMachine, persistence, cluster, executor, roleListener)
    role.startRole()
    role
  }


}
