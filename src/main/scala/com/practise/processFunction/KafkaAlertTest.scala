package com.practise.processFunction

import java.util.Properties

import com.practise.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector

object KafkaAlertTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "kafka-alert")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val inputStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))

    val datastream: DataStream[String] = inputStream.map(data => {
      val splitArr: Array[String] = data.split(",")
      SensorReading(splitArr(0), splitArr(1).toInt, splitArr(2).toLong)
    }).keyBy(_.id)
      .process(new KafkaAlertProcessFunction(2 * 60 * 1000L))
    datastream.print("kafkaAlert: ")

    env.execute("kafka alert test.....")
  }
}

class KafkaAlertProcessFunction(interval: Long) extends KeyedProcessFunction[String, SensorReading, String] {
  //获取事件的状态，上一个事件的状态
  val lastEventState: ValueState[SensorReading] = getRuntimeContext.getState(new ValueStateDescriptor[SensorReading]("lastEvent", Types.of[SensorReading]))


  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
    //获取上一个事件
    var lastEvent: SensorReading = lastEventState.value()

    //判断上一个事件是否存在，第一次是不存在的
    if (lastEvent == null) {
      lastEvent = value
    } else {
      lastEvent.timestamp = value.timestamp
      lastEvent.temperature = value.temperature
    }

    //更新lastEventState状态
    lastEventState.update(lastEvent)
    //注册timer，以系统时间为准，2分钟内接收数据的情况是基于系统本身时间，所以注册时是系统时间+interval
    ctx.timerService().registerEventTimeTimer(System.currentTimeMillis() + interval)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    //从状态中得到事件
    val lastEvent: SensorReading = lastEventState.value()
    //判断lastEvent是否为空以及触发时间大于了事件时间+interval，这样表示没有新数据使lastevent更新，那么就输出alert，有数据则正常，什么也不操作
    if (lastEvent == null || timestamp > lastEvent.timestamp + interval) {
      out.collect(s"kafka has no data in 2s, current event: ${lastEvent}")
      //已经输出了，删除当前的timer，否则一直存在
      ctx.timerService().deleteEventTimeTimer(timestamp)
      //注册下一个事件的timer
      ctx.timerService().registerEventTimeTimer(timestamp + interval)
      //更新当前事件的时间为触发时间
      lastEvent.timestamp = timestamp
      // 更新状态
      lastEventState.update(lastEvent)
    }
  }
}


