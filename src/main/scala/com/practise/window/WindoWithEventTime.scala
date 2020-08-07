package com.practise.window

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

object WindoWithEventTime {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //格式：id_ts_tmp
    val inputStream: DataStream[String] = env.socketTextStream("localhost", 7777)
    //将输入流转换格式，提取事件时间，转成keyed流
    val keyedStream: KeyedStream[(String, Long, Double), Tuple] = inputStream.map(data => {
      val splitArr: Array[String] = data.split("_")
      (splitArr(0), splitArr(1).toLong, splitArr(1).toDouble)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Long, Double)]() {
      override def extractTimestamp(element: (String, Long, Double)): Long = element._2
    }).keyBy(0)

    // keyed流进行开窗
    // 滚动窗口，窗口大小2秒
    //    val windowStream: WindowedStream[(String, Long, Double), Tuple, TimeWindow] = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(2)))
    //滑动窗口
    //    val windowStream: WindowedStream[(String, Long, Double), Tuple, TimeWindow] = keyedStream.window(SlidingEventTimeWindows.of(Time.seconds(5),Time.seconds(2)))
    //会话窗口
    val windowStream: WindowedStream[(String, Long, Double), Tuple, TimeWindow] = keyedStream.window(EventTimeSessionWindows.withGap(Time.milliseconds(500)))

    val reduceStream: DataStream[(String, Long, Double)] = windowStream.reduce((x, y) => {
      (x._1, x._2 + 1, y._3 + x._3)
    })
    reduceStream.print()

    env.execute("window with evet time....")
  }

}
