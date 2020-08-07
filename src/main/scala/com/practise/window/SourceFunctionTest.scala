package com.practise.window

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

import scala.util.Random

object SourceFunctionTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //定义自己的source，这个source中可以获得事件时间以及提交watermark
    val sourceStream: DataStream[String] = env.addSource(new MySource)
    val wordStream: DataStream[(String, Int)] = sourceStream.filter(_.nonEmpty).map(data => {
      val splitArr: Array[String] = data.split("-")
      (splitArr(0), 1)
    }).keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)

    wordStream.print()

    env.execute("sourcefunction test....")


  }
}

/**
  * SourceFunction的泛型String表示事件类型，在collectWithTimestamp方法中需要
  */
class MySource extends SourceFunction[String] {
  //定义判断停止的变量
  var running = true

  //这个方法就是从得到的数据中获取事件时间以及提交watermark
  //这里我们在Mysource中生成原始数据，然后从原始数据中提取时间
  //生产中可以接收kafka中的消息，然后提取时间并提交watermark
  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    while (running) {
      //每隔1秒钟生成一波数据
      Thread.sleep(1000)

      //造数据，数据格式是“随机字母-时间戳”
      val letter: Char = (65 + new Random().nextInt(25)).toChar
      val ts = System.currentTimeMillis()
      val event: String = s"${letter}_${ts}".toString

      //收集事件和时间
      ctx.collectWithTimestamp(event, ts)

      //提交watermark，指定时间间隔为1s
      ctx.emitWatermark(new Watermark(ts - 1000))
    }
  }

  override def cancel(): Unit = {
    running = false
  }

}