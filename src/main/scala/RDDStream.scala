import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * @author ZQ
  * @create 2018-12-04 20:30
  */
object RDDStream extends App {

  //初始化SparkConf
  private val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDS")

  //初始化SSC
  private val ssc = new StreamingContext(sparkConf,Seconds(3))

  //创建RDD队列
  private val RDDQue = new mutable.Queue[RDD[Int]]

  //创建输入流
  private val inputStream: InputDStream[Int] = ssc.queueStream(RDDQue,oneAtATime = false)

  //处理队列中的RDD数据
  private val mappedInput = inputStream.reduce(_+_)

  //打印结果
  mappedInput.print()

  //启动任务
  ssc.start()

  //循环创建RDD，放入到RDD队列
  for (i<-1 to 5){
    RDDQue+=ssc.sparkContext.makeRDD(1 to 5,8)
    Thread.sleep(1000)
  }



}
