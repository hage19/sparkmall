package com.atguigu.handler

import com.atguigu.utils.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

object DateAreaAdsTop3CountHandler {

  /**
   * 统计每天每个地区点击量在前3的广告并存入Redis
   *
   * @param dateAreaCityAdsToCount
   * @return
   */
  def saveDateAreaAdsTop3CountToRedis(dateAreaCityAdsToCount: DStream[(String, Long)]) = {

    //1.转换维度:（dateAreaCityAds=>dateAreaAds）
    val dateAreaAdsToCount: DStream[((String, String, String), Long)] = dateAreaCityAdsToCount.map { case (dateAreaCityAds, count) =>

      //s"$dateStr:${adsLog.area}:${adsLog.city}:${adsLog.adid}"
      val splits: Array[String] = dateAreaCityAds.split(":")

      ((splits(0), splits(1), splits(3)), count)
    }.reduceByKey(_ + _)

    //2.转换维度，将时间作为Key
    val dateToAreaAdsToCount: DStream[(String, (String, (String, Long)))] = dateAreaAdsToCount.map { case ((date, area, ads), count) =>

      //(时间，（地区，(广告，总数)))
      (date, (area, (ads, count)))
    }

    //3.按照时间进行分组
    val dateToAreaAdsToCountGroupByDate: DStream[(String, Iterable[(String, (String, Long))])] = dateToAreaAdsToCount.groupByKey()




    //4.时间组内按照地区进行分组（注意，地区分组为scala集合）
    val result: DStream[(String, Map[String, List[(String, Long)]])] = dateToAreaAdsToCountGroupByDate.mapValues(iter => {

      //a.按照地区分组
      val areaToAreaToAdsCount: Map[String, Iterable[(String, (String, Long))]] = iter.groupBy(_._1)

      //b.去掉分组内部的地区
      //      val tupleses: immutable.Iterable[Iterable[(String, (String, Long))]] = areaToAreaToAdsCount.map { case (area, items) =>
      //
      //        items
      //      }
      val areaAdsCountGroup: Map[String, Iterable[(String, Long)]] = areaToAreaToAdsCount.map { case (area, items) =>
        (area, items.map(_._2))
      }

      //c.排序（时间组大区组内对广告点击量进行排序并取前3）
      val areaAdsCountTop3List: Map[String, List[(String, Long)]] = areaAdsCountGroup.mapValues(items =>
        items.toList.sortWith { case (c1, c2) => c1._2 > c2._2 }.take(3)
      )
      areaAdsCountTop3List
    })

    //5.将时间地区组内的广告点击量集合转换为JSONStr
    //result: DStream[(String, Map[String, List[(String, Long)]])]
    val dateToAreaToAdsCountStr: DStream[(String, Map[String, String])] = result.mapValues { areaAdsCountMap =>

      import org.json4s.JsonDSL._

      areaAdsCountMap.mapValues(list =>

        //将list转换为jsonStr
        JsonMethods.compact(JsonMethods.render(list))
      )
    }

    //6.存入Redis
    dateToAreaToAdsCountStr.foreachRDD(rdd => {


      rdd.foreachPartition(items => {

        //判断传入数据是否为空

        if (items.nonEmpty) {

          //a.获取Redis连接
          val jedis: Jedis = RedisUtil.getJedisClient

          //b.遍历写入Redis
          items.foreach { case (date, map) =>

            import scala.collection.JavaConversions._

            jedis.hmset(s"top3_ads_per_day:$date", map)
          }

          //c.关闭Redis连接
          jedis.close()
        }
      })
    })


  }

}
