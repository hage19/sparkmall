package com.atguigu.handler

import com.alibaba.fastjson.JSONObject
import com.atguigu.datamode.UserVisitAction
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

/**
 *
 * @auther hage
 * @creat 2020-08-23 22:42
 */
object CategoryTop10Handler {
  def getCategoryTop10Session(userVisitActionRDD: RDD[UserVisitAction], result: List[(String, mutable.HashMap[String, Long])]): RDD[(Long, String, Int)] = {

    val categoryTop10: List[String] = result.map(_._1)

    val filteredUserActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(UserVisitAction => categoryTop10.contains(UserVisitAction.click_category_id.toString))


    val sessionAndCategoryToOne: RDD[((String, Long), Int)] = filteredUserActionRDD.map(userVisitAction => ((userVisitAction.session_id, userVisitAction.click_category_id), 1))

    val sessionAndCategoryToCount: RDD[((String, Long), Int)] = sessionAndCategoryToOne.reduceByKey(_ + _)
    //5.转换维度：RDD[(Session,Category),count]=>RDD[Category,(Session,count)]
    val categoryToSessionAndCount: RDD[(Long, (String, Int))] = sessionAndCategoryToCount.map {
      case ((session, category), count) => (category, (session, count))
    }
    //6.分组Category,groupByKey：RDD[Category,(Session,count)]=>RDD[Category,Iter[(Session,count)*n个]
    val categoryToSessionAndCountGroup: RDD[(Long, Iterable[(String, Int)])] = categoryToSessionAndCount.groupByKey()
    //7.组内排序并取前10：RDD[Category,Iter[(Session,count)...]=>RDD[Category,Iter[(Session,count)*10个]

    //def flatMap[U: ClassTag](f: T => TraversableOnce[U]): RDD[U] = withScope

    val categorySessionTop10RDD: RDD[(Long, String, Int)] = categoryToSessionAndCountGroup.flatMap { case (category, items) =>
      //排序
      val sortedList: List[(String, Int)] = items.toList.sortWith { case (sessionCount1, sessionCount2) =>
        sessionCount1._2 > sessionCount2._2
      }.take(10) //取前十

      val result: List[(Long, String, Int)] = sortedList.map { case (session, count) =>
        (category, session, count)
      }
      //返回
      result
    }
    categorySessionTop10RDD



  }


  def readAndFilterData(conditionObj: JSONObject, spark: SparkSession): RDD[UserVisitAction] ={

    import spark.implicits._

    val startDate: String = conditionObj.getString("startDate")
    val endDate: String = conditionObj.getString("endDate")
    val startAge: String = conditionObj.getString("startAge")
    val endAge: String = conditionObj.getString("endAge")

    println(startDate + endDate + startAge +endAge)

    val sql: StringBuilder = new StringBuilder("select v.* from user_visit_action v join user_info u on v.user_id = u.user_id where 1=1")

    if (startDate != null){
      sql.append(s" and date>= '$startDate'")
    }
    if (endDate != null){
      sql.append(s" and date<= '$endDate'")
    }
    if (startAge != null){
      sql.append(s" and age>= '$startAge'")
    }
    if (endAge != null){
      sql.append(s" and age>= '$endAge'")
    }

    println(sql)

    val df: DataFrame = spark.sql(sql.toString())


    df.show(10)

//    val rdd: RDD[Row] = df.rdd
    val userVisitActionRDD: RDD[UserVisitAction] = df.as[UserVisitAction].rdd

    userVisitActionRDD
  }






}
