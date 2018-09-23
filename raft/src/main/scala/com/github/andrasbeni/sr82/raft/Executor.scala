package com.github.andrasbeni.sr82.raft

import java.util.concurrent._

import org.slf4j.{Logger, LoggerFactory}


trait Executor extends AutoCloseable {
  def submit[T](callable : Callable[T]): Future[T]
  def schedule[T](callable : Callable[T], milliseconds : Long): Future[T]

}

class DefaultExecutor extends Executor {

  private val logger : Logger = LoggerFactory.getLogger(classOf[Executor])

  private def logging[T](callable : Callable[T]) : Callable[T] = {
    () => try {
      callable.call()
    } catch {
      case e : Exception =>
        logger.error("Error", e)
        throw e
    }
  }


  private val executor : ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()

  override def submit[T](callable : Callable[T]): Future[T] =
    executor.submit(logging(callable))

  override def schedule[T](callable : Callable[T], milliseconds : Long): Future[T] =
    executor.schedule(logging(callable), milliseconds, TimeUnit.MILLISECONDS)

  override def close(): Unit = executor.shutdownNow()

}

