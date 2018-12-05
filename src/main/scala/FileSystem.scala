import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author ZQ
  * @create 2018-12-04 22:20
  */
object FileSystem extends App {

  //初始化ssc
  private val ssc = new StreamingContext(new SparkConf().setAppName("FS").setMaster("local[*]"),Seconds(2))

  //创建自定义receiver的Streaming
  private val lineStream: ReceiverInputDStream[String] = ssc.receiverStream(new CustomerReceiver("hadoop107",9999))
  //4.将每一行数据做切分，形成一个个单词
  val wordStreams = lineStream.flatMap(_.split("\t"))

  //5.将单词映射成元组（word,1）
  val wordAndOneStreams = wordStreams.map((_, 1))

  //6.将相同的单词次数做统计
  val wordAndCount = wordAndOneStreams.reduceByKey(_ + _)

  //7.打印
  wordAndCount.print()

  //8.启动SparkStreamingContext
  ssc.start()
  ssc.awaitTermination()






}
