package com.practise.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
// windows 下使用nc命令，可以下载nc for windows，地址：https://eternallybored.org/misc/netcat/
// 下载  netcat 1.12 版本，然后放到C:\Users\当前账户 下即可，这是没配置环境变量方式，所以使用时要通过cmd进入的C盘目录
// 命令：nc -l -s localhost -p 7777
object WordCountStreaming {
  def main(args: Array[String]): Unit = {
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = params.get("host")
    val port: Int = params.getInt("port")

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val wcDataStream: DataStream[String] = env.socketTextStream(host, port)
    val wcResultDataStream: DataStream[(String, Int)] = wcDataStream.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    wcResultDataStream.print()

    env.execute("WordCount Streaming")
  }
}
