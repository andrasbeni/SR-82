package com.github.andrasbeni.sr82

import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.{CompletableFuture, Future, ScheduledFuture}

import com.github.andrasbeni.sr82.raft._
import com.sun.corba.se.spi.orbutil.fsm.Guard.Complement
import org.slf4j.{Logger, LoggerFactory, MDC}

trait RoleFactory {
  def apply(config : Properties, stateMachine : StateMachine[_, _], persistence : Persistence, cluster : Cluster, executor : Executor, roleListener : Role => Unit) : Role
}

/**
  * Created by andrasbeni on 11/10/17.
  */
abstract class Role(val config : Properties, val stateMachine : StateMachine[_, _], val persistence : Persistence, val cluster : Cluster, val executor : Executor, val roleListener : Role => Unit) {

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


  def requestVote(req: VoteRequest): VoteResponse = {
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
    logger.debug(s"Responding to vote request from ${req.getCandidateId}: ${new VoteResponse(oldTerm, granted)}")
    new VoteResponse(oldTerm, granted)
  }

  def appendEntries(req: AppendEntriesRequest) : AppendEntriesResponse

  private def candidateLogMoreUpToDate(req: VoteRequest): Boolean = {
    val lastEntry = persistence.log.lastEntry
    (req.getLastLogTerm > lastEntry.getTerm) ||
      (req.getLastLogTerm == lastEntry.getTerm &&
        req.getLastLogIndex >= lastEntry.getIndex)
  }

  def becomeFollower(leaderId : Int): Role = {
    cluster.leaderId = leaderId
    convertTo(Follower)
  }

  def changeState(x: ByteBuffer) : Future[ByteBuffer] = {
    val future = new CompletableFuture[ByteBuffer]
    completeWithNotLeader(future)
  }


  protected def completeWithNotLeader(future: CompletableFuture[ByteBuffer]): CompletableFuture[ByteBuffer] = {
    val notLeader = new NotLeader()
    notLeader.setLeaderAddress(cluster.currentLeader)
    future.completeExceptionally(notLeader)
    future
  }

  private var timer : Option[Future[_]]= None

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
