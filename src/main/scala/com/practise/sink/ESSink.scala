package com.practise.sink

import java.util

import com.practise.source.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

object ESSink {


  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //从文件中读取，放入es
    val inputStream: DataStream[String] = env.readTextFile("D:\\BigDataStudy\\workspase\\flink-learning\\src\\main\\resources\\sensor.txt")

    val dataStream: DataStream[SensorReading] = inputStream.map(data => {
      val splitArr: Array[String] = data.split(",")
      SensorReading(splitArr(0), splitArr(1).toLong, splitArr(2).toDouble)
    })

    var httpHosts: util.List[HttpHost] = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("localhost", 9200))

    val esSinkFunc: ElasticsearchSinkFunction[SensorReading] = new ElasticsearchSinkFunction[SensorReading] {

      override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        val source = new util.HashMap[String, String]()
        source.put("id", t.id)
        source.put("ts", t.timestamp.toString)
        source.put("temp", t.temperature.toString)

        var indexRequest = Requests.indexRequest().index("sensor").`type`("_doc").source(source)

        requestIndexer.add(indexRequest)
      }
    }


    dataStream.addSink(new ElasticsearchSink.Builder[SensorReading](httpHosts, esSinkFunc).build())

    env.execute("es sink....")
  }
}
