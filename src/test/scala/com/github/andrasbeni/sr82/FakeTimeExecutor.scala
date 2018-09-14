package com.github.andrasbeni.sr82
import java.util.PriorityQueue
import java.util.concurrent.{Callable, CompletableFuture, Future}

/**
  * Created by andrasbeni on 9/14/18.
  */
class FakeTimeExecutor extends Executor {

  var time : Long = 0

  class WorkItem[T](val time : Long, val task : Callable[T], val result : CompletableFuture[T])

  val workQueue = new PriorityQueue[WorkItem[_]]((i, j) => i.time.compareTo(j.time))

  def passTime(milliseconds : Long): Unit = {
    time += milliseconds
    while (!workQueue.isEmpty && workQueue.peek().time <= time) {
      complete(workQueue.poll())
    }
  }

  private def complete[T](item : WorkItem[T]) : Unit = {
    try {
      if (!item.result.isCancelled) {
        item.result.complete(item.task.call())
      }
    } catch {
      case e : Exception => item.result.completeExceptionally(e)
    }
  }


  override def submit[T](callable: Callable[T]): Future[T] = {
    schedule(callable, 1)
  }

  override def schedule[T](callable: Callable[T], milliseconds: Long): Future[T] = {
    val workItem = new WorkItem(time + milliseconds, callable, new CompletableFuture[T]())
    workQueue.add(workItem)
    workItem.result
  }

  override def close(): Unit = {}
}
