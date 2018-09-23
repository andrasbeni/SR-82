package com.github.andrasbeni.sr82.raft

import java.io.File
import java.nio.file.Files.createTempFile
import java.nio.file.attribute.PosixFilePermissions.{asFileAttribute, fromString}

import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.{Before, Test}

/**
  * Created by andrasbeni on 12/18/17.
  */
class StateTest {

  var state : State = _
  var file : String = _
  val defaultState = new VoteAndTerm(2017, 1218)

  @Before def before() {
    val tempFile = createTempFile("test", "log", asFileAttribute(fromString("rw-r-----"))).toFile
    tempFile.deleteOnExit()
    file = tempFile.getAbsolutePath
    state = new State(file, defaultState)
  }

  @Test def testDefaultStateNotWritten(): Unit = {
    assertEquals(0, new File(file).length())
  }

  @Test def testDefaultState {
    assertEquals(defaultState, state.get())
  }

  @Test def testNewStateIsWritten {
    val voteAndTerm = new VoteAndTerm(2000, 1)
    state.set(voteAndTerm)
    assertTrue(file.length > 0)
    assertEquals(voteAndTerm, state.get())
    assertEquals(voteAndTerm, state.read())
  }


  @Test def testStateIsOverwritten {
    val voteAndTerm = new VoteAndTerm(2000, 1)
    state.set(voteAndTerm)
    val voteAndTerm2 = new VoteAndTerm(2001, 2)
    state.set(voteAndTerm2)
    assertTrue(file.length > 0)
    assertEquals(voteAndTerm2, state.get())
    assertEquals(voteAndTerm2, state.read())
  }


  @Test def testCloseAndOpen {
    val voteAndTerm = new VoteAndTerm(2000, 1)
    state.set(voteAndTerm)
    state.close()
    state = new State(file, new VoteAndTerm(1000, 3))
    assertEquals(voteAndTerm, state.get())
    assertEquals(voteAndTerm, state.read())
  }


}
