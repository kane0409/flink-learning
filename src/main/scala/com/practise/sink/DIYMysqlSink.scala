package com.practise.sink

import java.sql
import java.sql.{DriverManager, PreparedStatement}

import com.practise.source.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object DIYMysqlSink {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    //从文件中读取数据，放入mysql中
    val inputStream: DataStream[String] = env.readTextFile("D:\\BigDataStudy\\workspase\\flink-learning\\src\\main\\resources\\sensor.txt")
    val dataStream: DataStream[SensorReading] = inputStream.map(data => {
      val splitArr: Array[String] = data.split(",")
      SensorReading(splitArr(0), splitArr(1).toLong, splitArr(2).toDouble)
    })

    dataStream.addSink(new MyMysqlSink())

    env.execute("diy mysql sink...")


  }
}

class MyMysqlSink() extends RichSinkFunction[SensorReading] {
  var conn: sql.Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    //获取mysql连接
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "root")
    //将数据插入mysql
    insertStmt = conn.prepareStatement("insert into sensor(id,temperature) values(?,?)")
    //更新mysql
    updateStmt = conn.prepareStatement("update sensor set temperature=? where id = ?")
  }

  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }

  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    updateStmt.setDouble(1, value.temperature)
    updateStmt.setString(2, value.id)
    updateStmt.execute()
    if (updateStmt.getUpdateCount == 0) {
      insertStmt.setString(1, value.id)
      insertStmt.setDouble(2, value.temperature)
      insertStmt.execute()
    }
  }
}
