package com.practise.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import scala.collection.immutable
import scala.util.Random

/**
  * 测试添加各种数据源
  */
object SourceTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //① 从集合中取数据，集合中的元素是类，即样例类，也可以直接将数据放入集合中
    val stream1: DataStream[SensorReading] = env.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.8),
      SensorReading("sensor_6", 1547718201, 15.4),
      SensorReading("sensor_7", 1547718202, 6.7),
      SensorReading("sensor_10", 1547718205, 38.1)
    ))
    stream1.print("stream1")

    //② fromElement
    val stream2: DataStream[Any] = env.fromElements(1, 0.34, "aaa", ("key", 1))
    stream2.print("stream2")

    //③ 从本地文件
    val stream3: DataStream[String] = env.readTextFile("D:\\BigDataStudy\\workspase\\flink-learning\\src\\main\\resources\\sensor.txt")
    stream3.print("stream3")

    //④ 从kafka
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "source-test-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    val stream4: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))
    stream4.print("stream4")

    //⑤ 自定义source
    val stream5: DataStream[SensorReading] = env.addSource(new MySensorSource())
    stream5.print("stream5")


    env.execute("source test...")
  }
}

// 定义样例类，表示传感器：id，时间戳，温度
case class SensorReading(id: String, timestamp: Long, temperature: Double)

/**
  * 自定义数据源，需要继承RichSourceFunction
  */
case class MySensorSource() extends RichSourceFunction[SensorReading] {
  //定义标签，表示数据源是否正常运行
  var running: Boolean = true

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    val random = new Random()
    //初始化数据，温度是随机值
    var curtemp: immutable.IndexedSeq[(String, Double)] = 1.to(10).map(
      i => ("sensor" + i, 60 + random.nextGaussian() * 20)
    )

    //一直运行，不停的生成新数据，一波一波的生成
    while (running) {
      // 更改数据，因是不断一波一波的生成数据的
      curtemp = curtemp.map(
        data => (data._1, data._2 + random.nextGaussian())
      )
      val timestamp: Long = System.currentTimeMillis()

      // 将每条数据放入到sourceContext中
      curtemp.foreach(
        data => sourceContext.collect(SensorReading(data._1, timestamp, data._2))
      )
      //暂停100毫秒后生成再生成一波随机数据
      Thread.sleep(100)
    }
  }

  override def cancel(): Unit = {
    running = false
  }
}
