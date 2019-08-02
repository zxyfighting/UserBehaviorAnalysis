import java.lang
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._
object NetworkTrafficAnalysis {
  def main(args: Array[String]): Unit = {
    /*统计每分钟的 ip 访问量，取出访问量最大的 5 个地址，每 5 秒更新一次*/
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val streamData: DataStream[String] = env.readTextFile("E:\\wokplace\\UserBehaviorAnalysis\\NetworkTrafficAnalysis\\src\\main\\resources\\apachetest.log")
    // streamData.print()
     val stream: DataStream[ApacheLogEvent] = streamData.map(line => {
     val linearray = line.split(" ")
     val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
     val timestamp = simpleDateFormat.parse(linearray(3)).getTime
     ApacheLogEvent(linearray(0), linearray(2), timestamp, linearray(5),
    linearray(6))
     })
     .assignTimestampsAndWatermarks(new
    BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.
    milliseconds(1000)) {
     override def extractTimestamp(t: ApacheLogEvent): Long = {
     t.eventTime
     }
     })
     .keyBy("url")
     .timeWindow(Time.minutes(10), Time.seconds(5))
     .aggregate(new CountAgg(), new WindowResultFunction())

   env.execute()

  }

  case class ApacheLogEvent(ip: String, userId: String, eventTime: Long,
                            method: String, url: String)



  class CountAgg  extends AggregateFunction[ApacheLogEvent,Long,Long]{
    override def createAccumulator(): Long = 0L

    override def add(in: ApacheLogEvent, acc: Long): Long = acc+1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc+acc1
  }
  case class UrlViewCount(url: String, windowEnd: Long, count: Long)
  class WindowResultFunction extends WindowFunction[Long,UrlViewCount,Tuple,TimeWindow]{
    override def apply(key: Tuple, window: TimeWindow, aggregateResult:
    Iterable[Long], collector: Collector[UrlViewCount]) : Unit = {
       val url: String = key.asInstanceOf[Tuple1[String]]._1
       val count = aggregateResult.iterator.next
       collector.collect(UrlViewCount(url, window.getEnd, count))
    }
  }

}
