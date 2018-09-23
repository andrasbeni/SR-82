package com.github.andrasbeni.sr82.raft.example

import java.io.{File, FileInputStream}
import java.nio.ByteBuffer
import java.util
import java.util.Properties

import com.github.andrasbeni.sr82.distributedmap.MapEntry
import com.github.andrasbeni.sr82.raft._

import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._



/**
  * Created by andrasbeni on 11/9/17.
  */
object MapServerDemo {

  def main(args: Array[String]): Unit = {

    val logger = LoggerFactory.getLogger("com")

    new File("target/").listFiles().foreach(_.delete())
    val servers = new util.HashMap[Int, Server](
      (1 to 3).map(i => {
        val props = new Properties
        props.load(new FileInputStream(s"src/main/resources/server$i.properties"))
        (i, new Server(props))
      }).toMap.asJava)
    servers.values.asScala.foreach(_.start())
    new Thread(() => {
      Thread.sleep(5000)
      val cl = new Client(List(("localhost", 55555), ("localhost", 55556), ("localhost", 55557)))
      (0 to 1000).foreach(i => {
        cl.put(new MapEntry((i % 117).toString, ByteBuffer.wrap(s"hello$i".getBytes())))
        Thread.sleep(4100)

        val s = new String(cl.get((i % 117).toString))
        if (s != s"hello$i") {
          throw new RuntimeException("" + i)
        }
        logger.info(s"Received ${s}")
      })
    }).start()
  }


}
