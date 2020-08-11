package com.practise.processFunction

import java.text.SimpleDateFormat

import com.practise.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/**
  * 2分钟内温度连续上升，则进行输出
  */
object TemperatureTest {
  def main(args: Array[String]): Unit = {
    // 获取环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 获取输入数据
    val inputStream: DataStream[String] = env.socketTextStream("localhost", 7777)

    val processStream: DataStream[String] = inputStream.map(data => {
      val splitArr: Array[String] = data.split(",")
      SensorReading(splitArr(0), splitArr(1).toInt, splitArr(2).toLong)
    }).keyBy(_.id)
      .process(new ContinueIncreaseTempAlertFunction(2 * 60 * 1000L))
    processStream.print("alert tmp: ")


    env.execute("processFuntion test....")
  }
}

class ContinueIncreaseTempAlertFunction(intervalTime: Long) extends KeyedProcessFunction[String, SensorReading, String] {
  // 获取上次的温度状态
  val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", Types.of[Double]))

  // 获取传感器的定时器时间戳的状态
  val lastTimerState: ValueState[Long] = getRuntimeContext.getState[Long](new ValueStateDescriptor[Long]("timer", Types.of[Long]))


  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
    // 获取状态中的数据，即温度和定时器时间戳，这里是取出上次的状态值，因下一步要更新这个状态，即覆盖
    val lastTemp: Double = lastTempState.value()
    val lastTimer: Long = lastTimerState.value()
    // 更新lastTempState，温度要取每次进来的事件的，timer的更新是要连续增长时采取更新，否则就删除了
    lastTempState.update(value.temperature)
    // 和当前事件中温度做比较，等于不处理，小于清除定时器，大于则更新两个状态
    if (value.temperature < lastTemp) {
      // 当前温度小于上个温度，那么就删除timer定时器，并清除当前定时器状态
      ctx.timerService().deleteEventTimeTimer(lastTimer)
      lastTimerState.clear()
    } else if (value.temperature > lastTemp) {
      //当前事件温度大于上一个温度了，更新timer和timer状态
      // 新timer = 当前时间 + intervalTime
      val newTimerTs: Long = ctx.timestamp() + intervalTime
      // 注册新timer
      ctx.timerService().registerEventTimeTimer(newTimerTs)
      // 更新lastTimerState状态
      lastTimerState.update(newTimerTs)
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 到达触发时间后，进行打印输出即可
    out.collect(s"currentTime: ${new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(timestamp)}, Sensor id: ${ctx.getCurrentKey}, countinue increase time: ${intervalTime}.")
    // 清空lastTimerState状态
    lastTimerState.clear()
  }
}





























