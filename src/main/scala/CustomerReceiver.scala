import java.io.{BufferedReader, InputStreamReader}

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import java.net.Socket
import java.nio.charset.StandardCharsets

/**
  * @author ZQ
  * @create 2018-12-04 21:17
  */
class CustomerReceiver(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
  //最初启动的时候，调用该方法，作用为：读数据并将数据发送给Spark
  override def onStart(): Unit = {
    new Thread("Socket Receiver") {
      override def run()= receive()
    }.start()
  }
  //读数据并将数据发送给Spark
  def receive(): Unit = {
    //创建一个Socket
    val socket = new Socket(host,port)

    //定义一个变量，用来接收端口传过来的数据
    var input :String= null

    //创建一个BufferREader用来读取端口传来的数据
    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream,StandardCharsets.UTF_8))

    //读取数据
    input= reader.readLine()

    //当receive没有关闭并且输入数据不为空，则循环发送数据给Spark
    while (!isStopped()&&input!=null){
      store(input)
      input = reader.readLine()
    }


    //跳出循环则关闭资源
    reader.close()
    socket.close()

    //重启任务
    restart("restart")

  }

  override def onStop(): Unit = {}
}
