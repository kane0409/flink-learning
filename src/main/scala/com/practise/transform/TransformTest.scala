package com.practise.transform

import com.practise.source.SensorReading
import org.apache.flink.api.common.functions.{FilterFunction, RichFlatMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, SplitStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

object TransformTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //sensor_1,1547718199,35.8
    val inputStream: DataStream[String] = env.readTextFile("D:\\BigDataStudy\\workspase\\flink-learning\\src\\main\\resources\\sensor.txt")
    //将每条数据转换成SensorReading对象
    val dataStream: DataStream[SensorReading] = inputStream.map(data => {
      val dataArr: Array[String] = data.split(",")
      SensorReading(dataArr(0), dataArr(1).toLong, dataArr(2).toDouble)
    })

    val minDataStream: DataStream[SensorReading] = dataStream.keyBy(_.id).minBy(2)

    val minDataStream2: DataStream[SensorReading] = dataStream.keyBy(_.id)
      .reduce((oldData, newData) => {
        SensorReading(newData.id, newData.timestamp + 1, oldData.temperature.min(newData.temperature))
      })

    val splitStream: SplitStream[SensorReading] = dataStream.split(data => {
      if (data.temperature > 30)
        Seq("high")
      else
        Seq("low")
    })
    val highStream: DataStream[SensorReading] = splitStream.select("high")
    val lowStream: DataStream[SensorReading] = splitStream.select("low")
    val totalStream: DataStream[SensorReading] = splitStream.select("high", "low")

    //union需要两个流的类型统一
    val unionStream: DataStream[SensorReading] = highStream.union(lowStream)

    //connect两个流类型可以不统一，然后通过map转换成统一的类型或格式
    val highWarningStream: DataStream[(String, Double)] = highStream.map(data => (data.id, data.temperature))
    val connectStream: ConnectedStreams[(String, Double), SensorReading] = highWarningStream.connect(lowStream)
    val coMapStream: DataStream[(String, Double, String)] = connectStream.map(fun1 => (fun1._1, fun1._2, "high"), fun2 => (fun2.id, fun2.temperature, "low"))

    val filterStream: DataStream[SensorReading] = dataStream.filter(new MyFilter())


    //print省略

    env.execute("transform test.....")

  }
}

class MyFilter() extends FilterFunction[SensorReading] {
  override def filter(t: SensorReading): Boolean = {
    t.id.startsWith("sensor_1")
  }
}

class MyFlatMap extends RichFlatMapFunction[Int, (Int, Int)] {
  var subtaskIndex = 0

  override def flatMap(in: Int, collector: Collector[(Int, Int)]): Unit = {
    if (in % 2 == subtaskIndex) {
      collector.collect((subtaskIndex, in))
    }
  }

  override def open(parameters: Configuration): Unit = {
    subtaskIndex = getRuntimeContext.getIndexOfThisSubtask
  }

  override def close(): Unit = super.close()
}
