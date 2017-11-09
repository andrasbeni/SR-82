package com.github.andrasbeni.rq

import java.io.{File, FileInputStream}
import java.util
import java.util.Properties

import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._



/**
  * Created by andrasbeni on 11/9/17.
  */
object RQ {

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
    new Thread() {override def run(){
    Thread.sleep(5000)
    val cl = new Client(List(("localhost", 55555), ("localhost", 55556), ("localhost", 55557)))
    var counter : Int = 0;
    while(true) {
      cl.add(s"hello-$counter".getBytes())
      Thread.sleep(4100)

      logger.info(s"Received ${new String(cl.next)}")
      cl.remove()
      counter +=1
    }
    }}.start()
//    new Thread(){
//      override def run(): Unit = {
//        Thread.sleep(10000)
//        while(true) {
//          val i = (3*math.random()+1).toInt
//          logger.info(s"closing($i)")
//          servers.get(i).close()
//          Thread.sleep(3000)
//          val props = new Properties
//          props.load(new FileInputStream(s"src/main/resources/server$i.properties"))
//
//          servers.put(i,new Server(props))
//          servers.get(i).start()
//          Thread.sleep(4100)
//
//        }
//      }
//    }.start()
  }


}
