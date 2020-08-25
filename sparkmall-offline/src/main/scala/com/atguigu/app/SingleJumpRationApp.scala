package com.atguigu.app

import java.util.{Properties, UUID}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.datamode.UserVisitAction
import com.atguigu.handler.SingleJumpRatioHandler
import com.atguigu.util.JdbcUtil
import com.atguigu.utils.PropertiesUtil
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 *
 * @auther hage
 * @creat 2020-08-24 20:43
 */
object SingleJumpRationApp {

  def main(args: Array[String]): Unit = {
    //导入隐式转换
//    import spark.implicits._

    //2.读取配置文件
    val properties: Properties = PropertiesUtil.load("conditions.properties")
    val condition: String = properties.getProperty("condition.params.json")

    //获取JSON对象
    val conditionObj: JSONObject = JSON.parseObject(condition)
    //获取跳转目标页面
    val targetPageFlow: String = conditionObj.getString("targetPageFlow")

    //1-7
    val targetPageFlowArray: Array[String] = targetPageFlow.split(",")

    //3.准备单页所需的过滤条件:1-6
    val singlePageArray: Array[String] = targetPageFlowArray.dropRight(1)

    //4.准备单跳所需的过滤条件(1-6).zip(2-7)=>(1-2,2-3...)
    val toPageArray: Array[String] = targetPageFlowArray.drop(1)
    val singleJumpPageArray: Array[String] = singlePageArray.zip(toPageArray).map {
      case (from, to) =>
        s"$from-$to"
    }
    singleJumpPageArray.foreach(println)

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SingleJumpRationApp")

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SingleJumpRationApp")
      .enableHiveSupport()
      .getOrCreate()


    import spark.implicits._

    val userVisitActionRDD: RDD[UserVisitAction] = spark.sql("select * from user_visit_action").as[UserVisitAction].rdd

    userVisitActionRDD.cache()

    val singlePageCount: RDD[(String, Long)] = SingleJumpRatioHandler.getSinglePageCount(userVisitActionRDD, singlePageArray)
    val singleJumpPageAndCount: RDD[(String, Long)] = SingleJumpRatioHandler.getSingleJumpCount(userVisitActionRDD, singleJumpPageArray)

    val task_id: String = UUID.randomUUID().toString

    val singlePageMap: Map[String, Long] = singlePageCount.collect().toMap
    val singleJumpPageAndCountArray: Array[(String, Long)] = singleJumpPageAndCount.collect()


    val result: Array[Array[Any]] = singleJumpPageAndCountArray.map { case (singleJump, count) =>
      val ratio: Double = count.toDouble / singlePageMap.getOrElse(singleJump.split("-")(0), 1L)
      Array(task_id, singleJump, ratio)
    }
    JdbcUtil.executeBatchUpdate("insert into jump_page_ratio values(?,?,?)",result)

    //11.断开连接
    spark.close()



  }

}
