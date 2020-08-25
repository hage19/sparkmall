package com.atguigu.handler


import com.atguigu.datamode.UserVisitAction
import org.apache.spark.rdd.RDD

object SingleJumpRatioHandler {

  /**
   * 获取单跳的点击次数
   *
   * @param userVisitActionRDD  原始数据集
   * @param singleJumpPageArray 过滤条件（1-2，2-3......）
   */
  def getSingleJumpCount(userVisitActionRDD: RDD[UserVisitAction], singleJumpPageArray: Array[String]): RDD[(String, Long)] = {

    //1.转换原始数据集
    val sessionToTimeAndPage: RDD[(String, (String, String))] = userVisitActionRDD.map(userVisitAction =>
      (userVisitAction.session_id, (userVisitAction.action_time, userVisitAction.page_id.toString))
    )

    //2.按照Session进行分组并排序
    val singleJumpPageAndOne: RDD[(String, Long)] = sessionToTimeAndPage.groupByKey().flatMap { case (session, items) =>

      //排序
      val sortedList: List[(String, String)] = items.toList.sortBy(_._1)

      //获取排序后的pageid
      val pageIds: List[String] = sortedList.map(_._2)

      //计算session内的单跳
      val fromPage: List[String] = pageIds.dropRight(1)
      val toPage: List[String] = pageIds.drop(1)
      val sessionJump: List[String] = fromPage.zip(toPage).map { case (from, to) =>
        s"$from-$to"
      }

      //按照指定的单跳条件进行过滤
      val filterList: List[String] = sessionJump.filter(singleJumpPageArray.contains)

      //返回
      filterList.map((_, 1L))
    }

    //3.求总数
    val singleJumpPageAndCount: RDD[(String, Long)] = singleJumpPageAndOne.reduceByKey(_ + _)

    //4.返回
    singleJumpPageAndCount

  }


  /**
   * 获取单页的点击次数
   *
   * @param userVisitActionRDD 原始数据集
   * @param singlePageArray    过滤条件
   */
  def getSinglePageCount(userVisitActionRDD: RDD[UserVisitAction], singlePageArray: Array[String]): RDD[(String, Long)] = {

    //1.按照指定的页面信息进行过滤
    val filterUserVisitActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(userVisitAction => {
      singlePageArray.contains(userVisitAction.page_id.toString)
    })

    //2.转换
    val singlePageCount: RDD[(String, Long)] = filterUserVisitActionRDD.map(userVisitAction =>
      (userVisitAction.page_id.toString, 1L)
    ).reduceByKey(_ + _)

    singlePageCount
  }

}
