package com.github.andrasbeni.sr82.raft

import java.nio.file.{Files, Path}
import Files.createTempFile
import java.io.File
import java.nio.ByteBuffer
import java.nio.file.attribute.PosixFilePermissions._

import org.junit.Test
import org.junit.Before
import org.junit.Assert._

/**
  * Created by andrasbeni on 12/18/17.
  */
class LogTest {

  var log : Log = _
  var file : String = _
  val defaultEntry = new LogEntry(2017L, 1218L, ZeroBytes())

  @Before def before() {
    val tempFile = createTempFile("test", "log", asFileAttribute(fromString("rw-r-----"))).toFile
    tempFile.deleteOnExit()
    file = tempFile.getAbsolutePath
    log = new Log(file, defaultEntry)
  }


  @Test def testEmptyLogDoesNotWrite {
    assertEquals(0, new File(file).length())
  }

  @Test def testEmptyLogHasZerothEntry {
    assertTrue(log.containsIndex(0))
    assertEquals(defaultEntry, log.lastEntry)
    assertEquals(Some(defaultEntry), log.read(0))
  }

  @Test def testEmptyLogHasNoFirstEntry {
    assertFalse(log.containsIndex(1))
    assertEquals(None, log.read(1))
  }

  @Test def testSimpleAppend {
    val entry = new LogEntry(1L, 1L, ByteBuffer.wrap("Hello".getBytes))
    log.append(1, entry.getData)
    resetValueBuffer(entry)
    assertTrue(log.containsIndex(1))
    assertEquals(entry, log.lastEntry)
    assertEquals(Some(entry), log.read(1))
  }

  @Test def testAppend {
    val entry = new LogEntry(1L, 1L, ByteBuffer.wrap("Hello".getBytes))
    log.append(entry)
    resetValueBuffer(entry)
    assertTrue(log.containsIndex(1))
    assertEquals(entry, log.lastEntry)
    assertEquals(Some(entry), log.read(1))
    assertFalse(new File(file).length() == 0)
  }

  @Test def testCloseAndOpen {
    var entry = new LogEntry(1L, 1L, ByteBuffer.wrap("Hello".getBytes))
    log.append(entry)
    resetValueBuffer(entry)
    entry = new LogEntry(2L, 1L, ByteBuffer.wrap("Raft".getBytes))
    log.append(entry)
    resetValueBuffer(entry)
    log.close()
    log = new Log(file, defaultEntry)
    assertTrue(log.containsIndex(2))
    assertEquals(entry, log.lastEntry)
    assertEquals(Some(entry), log.read(2))
  }
  
  @Test def testRollback() {
    val entry1 = new LogEntry(1L, 1L, ByteBuffer.wrap("Hello".getBytes))
    log.append(entry1)
    resetValueBuffer(entry1)
    val entry2 = new LogEntry(2L, 1L, ByteBuffer.wrap("Raft".getBytes))
    log.append(entry2)
    resetValueBuffer(entry2)
    val entry3 = new LogEntry(3L, 2L, ByteBuffer.wrap(Array.emptyByteArray))
    log.append(entry3)
    resetValueBuffer(entry3)
    log.rollback(3)
    log = new Log(file, defaultEntry)
    assertTrue(log.containsIndex(2))
    assertFalse(log.containsIndex(3))

    assertEquals(entry2, log.lastEntry)
    assertEquals(Some(entry2), log.read(2))

  }

  private def resetValueBuffer(entry : LogEntry) {
    entry.getData.rewind()
  }

}