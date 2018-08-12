package com.github.andrasbeni.sr82


import java.util.Properties

import com.github.andrasbeni.sr82.raft._

import scala.collection.JavaConverters._

object Follower extends RoleFactory {
  override def apply(config : Properties, stateMachine: StateMachine[_, _], persistence: Persistence, cluster: Cluster, executor: Executor, roleListener : Role => Unit): Role = {
    new Follower(config, stateMachine, persistence, cluster, executor, roleListener)
  }
}

class Follower(config : Properties, stateMachine : StateMachine[_, _], persistence : Persistence, cluster : Cluster, executor : Executor, roleListener : Role => Unit)
  extends Role(config, stateMachine, persistence, cluster, executor, roleListener) {

  override def startRole(): Unit = {
    super.startRole()
    startTimer()
  }

  override def appendEntries(req: AppendEntriesRequest): AppendEntriesResponse = {
    cancelTimer()
    var valid = req.getTerm >= persistence.term
    if (valid) {
      if (req.getTerm != persistence.term) {
        persistence.setVoteAndTerm(-1, req.getTerm)
      }
      valid = valid & doAppendEntries(req)
    }
    startTimer()
    new AppendEntriesResponse(persistence.term, valid)
  }

  def doAppendEntries(req: AppendEntriesRequest) : Boolean = {
    val log = persistence.log
    val prevEntry = log.read(req.getPrevLogIndex)
    val containsLeadersPrevEntry = prevEntry.exists(_.getTerm == req.getPrevLogTerm)
    if (containsLeadersPrevEntry) {
      val firstNewEntry = if (req.getEntries.isEmpty) None else Some(req.getEntries.get(0))
      val nextIndex = req.getPrevLogIndex + 1
      firstNewEntry.filter(_ => log.containsIndex(nextIndex)).foreach(_ => {
        log.rollback(nextIndex)
      })
      req.getEntries.asScala.foreach(entry =>log.append(entry)) ;
    }
    if (persistence.lastApplied < req.getLeaderCommit) {
      val lastToApply = math.min(req.getLeaderCommit, log.lastEntry.getIndex)
      stateMachine.applyToIndex(lastToApply)
      persistence.lastApplied = lastToApply
      persistence.commitIndex = req.getLeaderCommit
    }
    containsLeadersPrevEntry
  }


  override def beforeRequestVote() : Unit = cancelTimer()

  override def afterRequestVote() : Unit = startTimer()

  override def becomeFollower(newLeader : Int): Role = {
    cluster.leaderId = newLeader
    this
  }


}
