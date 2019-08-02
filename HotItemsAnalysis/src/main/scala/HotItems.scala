import java.lang
import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import scala.collection.mutable.ListBuffer
object HotItems {
  def main(args: Array[String]): Unit = {
    /* 求 某 个 窗 口 中 前 N 名 的 热 门 点 击 商 品，key 为 窗 口 时 间 戳， 输 出 为 TopN 的 结 果*/

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    /*获取数据源*/
    env.readTextFile("H:\\UserBehavior.csv")
      .map(line =>{
        val linearray = line.split(",")
        UserBehavior(linearray(0).toLong,
          linearray(1).toLong,
          linearray(2).toInt,
          linearray(3),
          linearray(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000)//指定时间戳和水位线
      .filter(_.behavior == "pv")
      .keyBy("itemId")
      .timeWindow(Time.seconds(60), Time.seconds(5))
      .aggregate(new CountAgg(), new WindowResultFunction())
      .keyBy("windowEnd")
      .process(new TopItems(3)).print()
    env.execute("hotItem")

  }


  //计算最热门 TopN 商品（每个窗口）
  class TopItems(topSize: Int) extends KeyedProcessFunction[Tuple,ItemViewCount,String]{

    private var itemState : ListState[ItemViewCount] = _

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      //命名状态变量的名字和状态变量的类型
      val itemStateDesc = new ListStateDescriptor[ItemViewCount]("itemstate",classOf[ItemViewCount])
      //定义状态变量
      val itemState = getRuntimeContext.getListState(itemStateDesc)

    }

    override def processElement(i: ItemViewCount,
                                context: KeyedProcessFunction[Tuple, ItemViewCount,
                                  String]#Context, collector: Collector[String]): Unit = {

       //将每条数据保存到状态中
      itemState.add(i)
      // 注 册 windowEnd+1 的 EventTime Timer, 当 触 发 时， 说 明 收 齐 了 属 于
      // 也 就 是 当 程 序 看 到windowend
      context.timerService().registerEventTimeTimer(i.windowEnd+1)

    }
     /*
     * 在定时器中实现在statte商品中的排序
     * */
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      super.onTimer(timestamp, ctx, out)
       //收起所有商品点击量
       val allItems: ListBuffer[ItemViewCount] = ListBuffer()
       import scala.collection.JavaConversions._
      for(item<- itemState.get()){
        allItems+=item
      }
      //清除状态中的数据
      itemState.clear()
      //将商品排序
      val sortedItems = allItems.sortBy(-_.count).take(topSize)
      //打印
      val result: StringBuilder = new StringBuilder
     result.append("====================================\n")
        .append("时间：").append(new Timestamp(timestamp-1)).append("\n")
    /*  for (elem <- sortedItems) {
        result.append(elem.itemId)
          .append(elem.count)
      }*/
      for(i <- sortedItems.indices){

           val currentItem: ItemViewCount = sortedItems(i)
         // e.g. No1： 商 品ID=12224 浏 览 量=2413
         result.append("No").append(i+1).append(":")
         .append(" 商 品ID=").append(currentItem.itemId)
       .append(" 浏 览 量=").append(currentItem.count).append("\n")
         }
       result.append("====================================\n\n")
       // 控 制 输 出 频 率， 模 拟 实 时 滚 动 结 果
      Thread.sleep(1000)
       out.collect(result.toString)

    }
  }























  /*
  userid:用户id
  itemId：
  categoryId
  behavior
  timestamp
   */
  case class UserBehavior(userId: Long, itemId: Long, categoryId: Int,
                          behavior: String, timestamp: Long)
  case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

  //得到每个商品在每个窗口中的点击量的数据流
  /*聚合函数实现，每一条数据加1*/

  class CountAgg extends AggregateFunction[UserBehavior, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
  }


  class WindowResultFunction extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
    override def apply(key: Tuple,
                       window: TimeWindow,
                       aggregateResult: Iterable[Long],
                       collector: Collector[ItemViewCount]): Unit = {
      val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0
      val count = aggregateResult.iterator.next
      collector.collect(ItemViewCount(itemId, window.getEnd, count))
    }

  }




}
