package com.github.andrasbeni.sr82

import java.util.concurrent.{Future, TimeUnit}

import org.junit.Assert._
import org.junit.{After, Before, Test}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionException


class ExecutorTest {

  var executor : Executor = _

  @Before def setup(): Unit = {
    executor = new Executor()
  }

  @After def tearDown() : Unit = {
    executor.close()
  }

  @Test def testSubmit() : Unit = {
    val result = executor.submit(() => 7)
    assertEquals(7, result.get())
  }

  @Test(expected = classOf[ExecutionException]) def testSubmitThrows() : Unit = {
    executor.submit(() => throw new RuntimeException).get()
  }


  @Test def testSchedule() : Unit = {
    val result = executor.schedule(() => 13, 2000)
    Thread.sleep(1800)
    assertFalse(result.isDone)
    Thread.sleep(300)
    assertTrue(result.isDone)
    assertEquals(13, result.get(0, TimeUnit.MILLISECONDS))
  }

  @Test(expected = classOf[ExecutionException]) def testScheduleThrows() : Unit = {
    executor.schedule(() => throw new RuntimeException, 100).get()
  }


  @Test def testSerialized() : Unit = {
    val events = new java.util.LinkedList[(Int, String)]()
    val futures = new java.util.LinkedList[Future[Int]]()
    (0 until 10).foreach(i => {
      val future = executor.submit(()=> {
        events.add((i, "Start"))
        Thread.sleep(300)
        events.add((i, "End"))
        i
      })
      futures.add(future)
    })
    futures.asScala.foreach(_.get())
    (0 until 10).foreach(i=> {
      assertEquals("Start and End should belong to the same index",
        events.get(2 * i)._1, events.get(2 * i + 1)._1)
      assertEquals("Start", events.get(2 * i)._2)
      assertEquals("End", events.get(2 * i + 1)._2)
    })
  }

  @Test(expected = classOf[Exception]) def testClose() : Unit = {
    executor.close()
    executor.submit(()=>3)
  }

}
