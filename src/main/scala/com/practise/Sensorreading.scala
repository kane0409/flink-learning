package com.practise

// 定义样例类，表示传感器：id，时间戳，温度
case class SensorReading(id: String, var timestamp: Long, var temperature: Double)
