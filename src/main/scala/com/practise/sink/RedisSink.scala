package com.practise.sink

import java.util.Properties

import com.practise.source.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object RedisSink {
  def main(args: Array[String]): Unit = {
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = params.get("host")
    val port: Int = params.getInt("port")

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //从kafka读取数据，放入redis中
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "sink-test-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val inputStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))
    val dataStream: DataStream[SensorReading] = inputStream.map(data => {
      val splitArr: Array[String] = data.split(",")
      SensorReading(splitArr(0), splitArr(1).toLong, splitArr(2).toDouble)
    })

    //设置redis
    val conf: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost(host).setPort(port).build()

    dataStream.addSink(new RedisSink[SensorReading](conf, MyRedisMapper()))

    env.execute("redis sink....")


  }
}

case class MyRedisMapper() extends RedisMapper[SensorReading] {
  override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.HSET, "sensor_temperature")

  override def getKeyFromData(t: SensorReading): String = t.id

  override def getValueFromData(t: SensorReading): String = t.temperature.toString
}
























