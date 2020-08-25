package com.atguigu.app

import java.util.UUID

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.datamode.UserVisitAction
import com.atguigu.accu.CategoryCountAccumlator
import com.atguigu.handler.CategoryTop10Handler
import com.atguigu.utils.PropertiesUtil
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
 *
 * @auther hage
 * @creat 2020-08-23 21:55
 */
object CategoryTop10 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("CategoaryTop10")

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("CategoaryTop10")
      .enableHiveSupport()
      .getOrCreate()
    val conditionsPro = PropertiesUtil.load("conditions.properties")
    val conditionJson: String = conditionsPro.getProperty("condition.params.json")
    val conditionObj: JSONObject = JSON.parseObject(conditionJson)
//    val value: Any = conditionObj.get("startDate")
//    println(value)
    val userVisitActionRDD: RDD[UserVisitAction] = CategoryTop10Handler.readAndFilterData(conditionObj, spark)

    //userVisitActionRDD.foreach(println)
    println(userVisitActionRDD.getNumPartitions)

    println(userVisitActionRDD.count())
    println(userVisitActionRDD.first())
    //UserVisitAction(2019-11-26,42,cc80a642-21c5-4e20-a953-43d470c63093,43,2019-11-26 00:00:00,null,9,73,null,null,null,null,26)

    val accumlator = new CategoryCountAccumlator

    spark.sparkContext.register(accumlator,"categoaryCount")

    userVisitActionRDD.foreach(userVisitAction=>{
      if(userVisitAction.click_category_id != -1){
        accumlator.add(s"click_${userVisitAction.click_category_id}")
      } else if (userVisitAction.order_category_ids != null){
        userVisitAction.order_category_ids.split(",").foreach(order_id=> accumlator.add(s"order_${order_id}"))
      }else if (userVisitAction.pay_category_ids !=null){
        userVisitAction.pay_category_ids.split(",").foreach(
          (pay_id => accumlator.add(s"pay_${pay_id}"))
        )
      }
    })

    val categoaryCountMap: mutable.HashMap[String, Long] = accumlator.value
    //(pay_7,139)
    //(click_19,652)
    //(order_17,176)
    categoaryCountMap.foreach(println)
//(12,Map(click_12 -> 654, order_12 -> 178, pay_12 -> 145))
    val categoryCountMapGroup: Map[String, mutable.HashMap[String, Long]] = categoaryCountMap.groupBy(_._1.split("_")(1))
    println("---")
    categoryCountMapGroup.foreach(println)

    val result: List[(String, mutable.HashMap[String, Long])] = categoryCountMapGroup.toList.sortWith { case (c1, c2) =>

      val category1: String = c1._1
      val category1Count: mutable.HashMap[String, Long] = c1._2
      val category2: String = c2._1
      val category2Count: mutable.HashMap[String, Long] = c2._2
      if (category1Count.getOrElse(s"click_$category1", 0L) > category2Count.getOrElse(s"click_$category2", 0L)) {
        true
      } else if (category1Count.getOrElse(s"click_$category1", 0L) == category2Count.getOrElse(s"click_$category2", 0L)) {
        if (category1Count.getOrElse(s"order_$category1", 0L) > category2Count.getOrElse(s"order_$category2", 0L)) {
          true
        } else if (category1Count.getOrElse(s"order_$category1", 0L) == category2Count.getOrElse(s"order_$category2", 0L)) {
          category1Count.getOrElse(s"pay_$category1", 0L) > category2Count.getOrElse(s"pay_$category2", 0L)
        } else {
          false
        }
      } else {
        false
      }
    }.take(10)
    println("result")
    result.foreach(println)
    //(15,Map(click_15 -> 688, pay_15 -> 131, order_15 -> 208))


    val taskId: String = UUID.randomUUID().toString
    //(15,Map(click_15 -> 688, pay_15 -> 131, order_15 -> 208))
    val categoryCountTop10Array: List[Array[Any]] = result.map {
      case (category, categoryCount) =>
        Array(taskId, category, categoryCount.getOrElse(s"click_$category", 0L), categoryCount.getOrElse(s"order_$category", 0L), categoryCount.getOrElse(s"pay_$category", 0L))
    }

//    println(categoryCountTop10Array.foreach(_.foreach(println)))
    //13.插入MySQL
//    JdbcUtil.executeBatchUpdate("insert into category_top10 values(?,?,?,?,?)", categoryCountTop10Array)
//    println("需求一完成！！！")


    //*************************************************************
    //获取前十品类的前十Session
    val categorySessionTop10: RDD[(Long, String, Int)] = CategoryTop10Handler.getCategoryTop10Session(userVisitActionRDD, result)

    //拉取到Driver端进行写库
    val c10: Array[(Long, String, Int)] = categorySessionTop10.collect()

    //写库
    val sqlArrays: Array[Array[Any]] = c10.map { case (category, session, count) =>
      Array(taskId, category, session, count)
    }
//    JdbcUtil.executeBatchUpdate("insert into category_session_top10 values(?,?,?,?)", sqlArrays)

    println("需求二完成！！！")

    c10.foreach(println)

  }


}
