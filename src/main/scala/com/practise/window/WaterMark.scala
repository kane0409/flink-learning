package com.practise.window

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

object WaterMark {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    //设置事件时间，数据源中一定要有表示事件时间的时间戳
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //因是周期性提交watermark，要设置watermark产生的时间间隔
    env.getConfig.setAutoWatermarkInterval(5000)

    // 从外部获取数据源
    val inputStream: DataStream[String] = env.socketTextStream("localhost", 7777)

    // 对数据进行整理，得到dataStream，元素格式：（A-时间戳）
    val dataStream: DataStream[(String, Long)] = inputStream.filter(_.nonEmpty).map(data => {
      val splitArr: Array[String] = data.split("-")
      (splitArr(0), splitArr(1).toLong)
    })

    // 提取时间戳
    val assignStream: DataStream[(String, Long)] = dataStream.assignTimestampsAndWatermarks(new MyPeriodicAssigner)

    // 使用BoundedOutOfOrdernessTimestampExtractor
    val assignStream2: DataStream[(String, Long)] = dataStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.seconds(5000)) {
      override def extractTimestamp(element: (String, Long)): Long = element._2
    })

    // 使用assignAscendingTimestamps
    val assignStream3: DataStream[(String, Long)] = dataStream.assignAscendingTimestamps(_._2)

    // 使用AssignerWithPunctuatedWatermarks
    val assignStream4: DataStream[(String, Long)] = dataStream.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks[(String, Long)] {
      override def checkAndGetNextWatermark(lastElement: (String, Long), extractedTimestamp: Long): Watermark = {
        if (lastElement._1.equals("A")) {
          new Watermark(lastElement._2 - 1000)
        } else {
          null
        }
      }

      override def extractTimestamp(element: (String, Long), previousElementTimestamp: Long): Long = element._2
    })


    val wordStrem: DataStream[(String, Long)] = assignStream.keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .sum(1)

    wordStrem.print()

    env.execute("AssignerWithPeriodicWatermarks....")


  }
}

class MyPeriodicAssigner extends AssignerWithPeriodicWatermarks[(String, Long)] {
  // 延时1分钟
  val bound = 60 * 1000
  // 观察到的最大时间戳
  var maxTs = Long.MinValue

  // 最大时间戳-延迟时间=水位线
  override def getCurrentWatermark: Watermark = {
    new Watermark(maxTs - bound)
  }

  override def extractTimestamp(element: (String, Long), previousElementTimestamp: Long): Long = {
    // 将最大时间戳赋值给maxTs
    maxTs = maxTs.max(element._2)
    // 提取元素中的时间戳
    element._2
  }
}

