package com.atguigu.udf

import com.atguigu.bean.CityRatio
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class CityRatioUDAF extends UserDefinedAggregateFunction {

  //输入数据类型
  override def inputSchema: StructType = StructType(StructField("city_name", StringType) :: Nil)
//  StructType.apply(StructField("",))

  //缓存数据类型
  override def bufferSchema: StructType = StructType(StructField("city_count", MapType(StringType, LongType)) :: StructField("total_count", LongType) :: Nil)

  //输出数据类型
  override def dataType: DataType = StringType

  //函数稳定性
  override def deterministic: Boolean = true

  //对缓存进行初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {

    buffer(0) = Map[String, Long]()

    buffer(1) = 0L

  }

  //区内合并
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

    //获取第一个字段：city_count:[(北京->5),(天津->7),(,,,->...)]
    val city_count: Map[String, Long] = buffer.getAs[Map[String, Long]](0)

    //取出map中传入城市对应value
    val count: Long = city_count.getOrElse(input.getString(0), 0L)

    buffer(0) = city_count + (input.getString(0) -> (count + 1L))

    //记录总数
    buffer(1) = buffer.getLong(1) + 1L

  }

  //区间合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {

    //1.取出两个buffer中的数据
    val city_count1: Map[String, Long] = buffer1.getAs[Map[String, Long]](0)
    val city_count2: Map[String, Long] = buffer2.getAs[Map[String, Long]](0)

    val total_count1: Long = buffer1.getLong(1)
    val total_count2: Long = buffer2.getLong(1)

    //2.各个城市总数相加
    //casecase (city_count2, (city_count1|city_name, count)) =>返回city_count2
    buffer1(0) = city_count1.foldLeft(city_count2) { case (map, (city_name, count)) =>
      map + (city_name -> (map.getOrElse(city_name, 0L) + count))
    }

    //3.总数相加
    buffer1(1) = total_count1 + total_count2

  }

  //计算结果:北京21.2%，天津13.2%，其他65.6%
  override def evaluate(buffer: Row): String = {

    //1.取出各城市点击次数及大区总数
    val city_count: Map[String, Long] = buffer.getAs[Map[String, Long]](0)
    val total_count: Long = buffer.getLong(1)

    //2.对城市点击次数进行排序并取出前2名
    val sortedCityCount: List[(String, Long)] = city_count.toList.sortWith { case (c1, c2) =>
      c1._2 > c2._2
    }.take(2)

    //3.求其他城市总的占比
    var otherRatio = 100D

    //4.求前2名城市占比
    val cityRatioList: List[CityRatio] = sortedCityCount.map { case (city, count) =>
      val city_ratio: Double = Math.round(count.toDouble * 1000 / total_count) / 10D
      otherRatio -= city_ratio
      CityRatio(city, city_ratio)
    }

    //5.将其他城市总占比添加至cityRatioList
    val ratios: List[CityRatio] = cityRatioList :+ CityRatio("其他", Math.round(otherRatio * 10) / 10D)

    //6.拼接字符串
    val str: String = ratios.mkString(",")

    //7.返回
    str
  }
}
