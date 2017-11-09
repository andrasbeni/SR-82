package com.github.andrasbeni.rq

import java.nio.ByteBuffer
import java.util
import java.util.Properties
import java.util.concurrent.Future
import java.util.concurrent.CompletableFuture

import com.github.andrasbeni.rq.proto._

import scala.collection.JavaConverters._

object Leader extends RoleFactory {
  override def apply(config : Properties, stateMachine: StateMachine, persistence: Persistence, cluster: Cluster, executor: Executor, roleListener : Role => Unit): Role = {
    new Leader(config, stateMachine, persistence, cluster, executor, roleListener)
  }
}

class Leader(config : Properties, stateMachine : StateMachine, persistence : Persistence, cluster : Cluster, executor : Executor, roleListener : Role => Unit)
  extends Role(config, stateMachine, persistence, cluster, executor, roleListener) {

  val nextIndex: util.Map[Int, Long] = new util.HashMap[Int, Long](cluster.serverIds.map((_, persistence.log.lastEntry.getIndex+1)).toMap.asJava)

  val matchIndex: util.Map[Int, Long] = new util.HashMap[Int, Long](cluster.serverIds.map((_, 0L)).toMap.asJava)

  val waitingForCommit : util.Map[Long, CompletableFuture[AddOrRemoveResp]] = new util.HashMap[Long, CompletableFuture[AddOrRemoveResp]]()

  val replicationCounters : util.Map[Long, Int] = new util.HashMap[Long, Int]()

  override def startRole(): Unit = {
    super.startRole()
    cluster.leaderId = cluster.localId
    sendAppendEntries()
    startTimer()
  }
  override def stopRole(): Unit = {
    super.stopRole()
    waitingForCommit.values().asScala.foreach(_.complete(new AddOrRemoveResp(cluster.currentLeader, false)))
  }

  override def onTimeout() : Unit = {sendAppendEntries(); startTimer()}

  private val heartbeatTimeout = persistence.config.getProperty("heartbeat.timeout").toLong

  override def timeout: Long = heartbeatTimeout

  def commit(index : Long) : Unit ={
    logger.debug(s"committing index $index")
    stateMachine.applyToIndex(index)
    persistence.commitIndex = math.max(persistence.commitIndex, index)
    persistence.lastApplied = math.max(persistence.lastApplied, index)
    waitingForCommit.get(index).complete(new AddOrRemoveResp(cluster.currentLeader, true))
  }

  def sendAppendEntries() : Unit = {
    def requestForNode(node : Int) : AppendEntriesReq = {
      val nextIndexForNode = nextIndex.get(node)
      val lastEntryAtNode = persistence.log.read(nextIndexForNode - 1).get
      val leaderCommit : Long = persistence.commitIndex
      val entries : util.List[LogEntry] = persistence.entriesFrom(nextIndexForNode).toList.asJava
      new AppendEntriesReq(persistence.term, cluster.localId, lastEntryAtNode.getIndex, lastEntryAtNode.getTerm,
      entries, leaderCommit)
    }
    def sendRequestToNode(node: Int, req: AppendEntriesReq) = {
      logger.debug(s"Sending append request to node $node : $req")
      cluster.clientTo(node).appendEntries(req, new Hollaback[AppendEntriesResp](s"Could not send appendEntries request to $node.", result => {
        logger.debug(s"Received append result from $node: $result")
        if (!alive) {
          logger.debug(s"Ignoring late response from $node")
        } else if (result.getSuccess) {
          req.getEntries.asScala.
            map(_.getIndex).
            map(index => {replicationCounters.put(index, replicationCounters.get(index) + 1); index}).
            filter(replicationCounters.get(_) > cluster.size / 2).foreach(commit(_))
          nextIndex.put(node, nextIndex.get(node) + req.getEntries.size())
        } else if (result.getTerm >= persistence.term) {
          becomeFollower(node)
        } else {
          matchIndex.put(node, matchIndex.get(node) - 1)
        }
      }))

    }
    cluster.serverIds.foreach(node => {
      for (index <- nextIndex.get(node) to persistence.log.lastEntry.getIndex) {
        replicationCounters.putIfAbsent(index, 0)
      }
      val req = requestForNode(node)
      sendRequestToNode(node, req)
    })
  }

  override def appendEntries(req: AppendEntriesReq): AppendEntriesResp = {
    val valid = req.getTerm > persistence.term
    if (valid) {
      persistence.setVoteAndTerm(-1, req.getTerm)
      val nextRole = becomeFollower(req.getLeaderId)
      nextRole.appendEntries(req)
    } else {
      new AppendEntriesResp(persistence.term, false)
    }
  }

  override def next(): NextResp = {
    logger.debug("Received next call")
    val next = stateMachine.next()
    new NextResp(cluster.currentLeader, next.isDefined, next.getOrElse(ZeroBytes()))
  }

  override def remove(): Future[AddOrRemoveResp] = append(new AddOrRemove(Operation.Remove, ZeroBytes()))

  override def add(value: ByteBuffer): Future[AddOrRemoveResp] = append(new AddOrRemove(Operation.Add, value))

  def append(command : AddOrRemove) : Future[AddOrRemoveResp] = {
    logger.debug(s"Received command $command")
    val index = persistence.log.append(persistence.term, command)
    val future = new CompletableFuture[AddOrRemoveResp]()
    waitingForCommit.put(index, future)
    future
  }

}
