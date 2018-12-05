import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author ZQ
  * @create 2018-12-04 9:38
  */
object WordCount extends App {

  //初始化Spark配置信息
  val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

  //初始化SSC
  val ssc = new StreamingContext(sparkConf,Seconds(2))

  //监控文件夹创建Dstream
  val dirStream: DStream[String] = ssc.textFileStream("D:\\SSC")

  //将每一行数据切分，形成一个个单词
  val wordStream: DStream[String] = dirStream.flatMap(_.split("\t"))

  //将单词隐射成元祖
  val wordT: DStream[(String, Int)] = wordStream.map((_,1))

  val WC: DStream[(String, Int)] = wordT.reduceByKey(_+_)

  //DStream 可以 transform，如下：可以看到transform方法中直接将rdd暴露了出来，我们可以在这个方法中直接对rdd进行算子操作。
  WC.transform(rdd=>rdd.filter(_._2<2)).print() //过滤掉次数小于2的word


//  WC.print()

  //启动SparkStreamingContext
  ssc.start()
  ssc.awaitTermination()







}
