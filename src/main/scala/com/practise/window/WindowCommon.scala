package com.practise.window

import com.practise.SensorReading
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

object WindowCommon {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    val inputStream: DataStream[String] = env.socketTextStream("localhost", 7777)

    //滚动窗口
    val minTempStream: DataStream[SensorReading] = inputStream.map(data => {
      val splitArr: Array[String] = data.split(",")
      SensorReading(splitArr(0), splitArr(1).toLong, splitArr(2).toDouble)
    }).keyBy(_.id)
      .timeWindow(Time.seconds(5))
      .reduce((x, y) => SensorReading(x.id, y.timestamp, x.temperature.min(y.temperature)))
    minTempStream.print("minTemp")

    //滑动窗口
    val minStream2: DataStream[(String, Double)] = inputStream.map(data => {
      val splitArr: Array[String] = data.split(",")
      (splitArr(0), splitArr(1).toDouble)
    }).keyBy(_._1)
      .timeWindow(Time.seconds(5), Time.seconds(3))
      .reduce((x, y) => (x._1, x._2.min(y._2)))
    minStream2.print("minTemp2")

    //countWindow
    val minStream3: DataStream[(String, Double)] = inputStream.map(data => {
      val splitArr: Array[String] = data.split(",")
      (splitArr(0), splitArr(1).toDouble)
    }).keyBy(_._1)
      .countWindow(5)
      .reduce((x, y) => (x._1, x._2.min(y._2)))
    minStream3.print("minTemp3")

    //countWindow的滑动窗口
    //第二个参数表示收到两个相同的key的数据时就计算一次，计算的范围是第一个参数
    val sumStream: DataStream[(String, String)] = inputStream.map(data => {
      val splitArr: Array[String] = data.split(",")
      (splitArr(0), splitArr(1))
    }).keyBy(_._1)
      .countWindow(10, 2)
      .sum(1)
    sumStream.print("sumStream")


    env.execute("window common....")

  }
}
