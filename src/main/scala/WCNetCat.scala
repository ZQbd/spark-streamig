import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author ZQ
  * @create 2018-12-04 20:04
  */
object WCNetCat extends App {

  //创建sparkConf配置文件
  private val sparkConf: SparkConf = new SparkConf().setAppName("WCNetCat").setMaster("local[*]")

  //创建ssc
  private val ssc = new StreamingContext(sparkConf,Seconds(3))

  //数据源,监听9999端口
  private val lineStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop107",9999)

  //将读进来的数据切分，形成一个个单词
  private val words: DStream[String] = lineStream.flatMap(_.split(" "))

  //将单词映射成元组,并聚合
  private val result: DStream[(String, Int)] = words.map((_,1)).reduceByKey(_+_)

  //打印
  result.print()

  //启动SparkStreamingConext
  ssc.start()
  ssc.awaitTermination()


}
