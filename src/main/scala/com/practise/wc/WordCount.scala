package com.practise.wc

import org.apache.flink.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val filePath = "D:\\BigDataStudy\\workspase\\flink-learning\\src\\main\\resources\\wc.txt"
    val wcDataSet: DataSet[String] = env.readTextFile(filePath)
    val wcResult: AggregateDataSet[(String, Int)] = wcDataSet.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .groupBy(0)
      .sum(1)
    wcResult.print()
  }
}
