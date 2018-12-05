import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author ZQ
  * @create 2018-12-05 0:04
  */
object StateWC extends App {

  //ssc
  private val ssc = new StreamingContext(new SparkConf().setAppName("SWC").setMaster("local[*]"),Seconds(2))
  ssc.checkpoint("./")

  //定义更新状态方法，参数value为当前操作的流数据，state为之前的已经处理的数据
  val UpdateFunc=(value:Seq[Int],state:Option[Int])=>{
    val currentCount: Int = value.sum
    val previousCount: Int = state.getOrElse(0)
    Some(currentCount+previousCount)
  }

  //创建Dstream
  private val lineStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop107",9999)
  private val pairs: DStream[(String, Int)] = lineStream.flatMap(_.split(" ")).map((_,1))

  //使用updateStateBykey来跟新状态
  private val stateDstream: DStream[(String, Int)] = pairs.updateStateByKey(UpdateFunc)

  //窗口函数

  //
//  private val ssss: Any = pairs.reduceByKeyAndWindow((x:Int,y:Int)=>x+y,Seconds(30),Seconds(10))
  //
  stateDstream.reduceByKey(_+_).print()

  ssc.start()
  ssc.awaitTermination()





}
