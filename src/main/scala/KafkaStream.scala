
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author ZQ
  * @create 2018-12-04 22:59
  */
object KafkaStream extends App {

  private val ssc = new StreamingContext(new SparkConf().setMaster("local[*]").setAppName("KafakaS"),Seconds(2))

  //定义Kafka参数

  val zookeeper ="hadoop107:2181,hadoop106:2181,hadoop105:2181"
  val topic = "source"
  val consumerGroup = "spark"

  //将kafka参数映射成map
  private val KafkaPara: Map[String, String] = Map[String, String](ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->

    "org.apache.kafka.common.serialization.StringDeserializer", ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.GROUP_ID_CONFIG -> consumerGroup,
    "zookeeper.connect" -> zookeeper
  )
  KafkaPara

  //通过KafkaUtil创建KafkaDStream
  private val KafkaDstream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc,KafkaPara,Map[String, Int](topic -> 3),StorageLevel.MEMORY_ONLY)

  //对KafkaDstream做计算
  KafkaDstream.foreachRDD(
    rdd=>rdd.flatMap(_._2.split(" ")).map((_,1)).reduceByKey(_+_).foreach(println)
  )

  //启动SparkStreaming
  ssc.start()
  ssc.awaitTermination()

}
