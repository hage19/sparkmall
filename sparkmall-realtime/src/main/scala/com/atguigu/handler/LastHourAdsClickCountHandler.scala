package com.atguigu.handler
import java.lang.ProcessBuilder.Redirect
import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.bean.AdsLog
import com.atguigu.utils.RedisUtil
import org.apache.spark.streaming.{Minutes, Seconds}
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

object LastHourAdsClickCountHandler {

  private val sdf = new SimpleDateFormat("HH:mm")

  private val redisKey = "last_hour_ads_click"

  /**
   * 统计各广告最近 1 小时内的点击量趋势：各广告最近 1 小时内各分钟的点击量并存入Redis
   *
   * @param filterdDStream 过滤后的原始数据
   * @return
   */
  def saveLastHourClickCountToRedis(filterdDStream: DStream[AdsLog]): Unit = {

    //1.转换维度：adsLog=>((adsId,hourMin),1L)
    val adsHourToOne: DStream[((String, String), Long)] = filterdDStream.map(adsLog => {
      //获取小时分钟
      val hourMinu: String = sdf.format(new Date(adsLog.timestamp))

      ((adsLog.adid, hourMinu), 1L)
    })

    //2.窗口内求和：((ads,hourMin),1L)=>((ads,hourMin),count)

    val adsHourToCount: DStream[((String, String), Long)] = adsHourToOne.reduceByKeyAndWindow((x: Long, y: Long) => x + y, Minutes(3), Minutes(1))

    //3.转换维度，按照广告id进行分组：((ads,hourMin),count)=>((ads,(hourMin,count))=>((ads,Iter[(hourMin,count)])
    val adsToHourCountGroup: DStream[(String, Iterable[(String, Long)])] = adsHourToCount.map { case ((ads, hourMin), count) => (ads, (hourMin, count)) }.groupByKey()

    //4.转换Iter[(hourMin,count)]为JSON：((ads,Iter[(hourMin,count)])=>((ads,Json))
    val adsToHourCountStr: DStream[(String, String)] = adsToHourCountGroup.mapValues(list => {

      import org.json4s.JsonDSL._

      JsonMethods.compact(JsonMethods.render(list))
    })

    //5.写入Redis：last_hour_ads_click,(ads,Json)
    adsToHourCountStr.foreachRDD(rdd => {

      rdd.foreachPartition { items =>

        if (items.nonEmpty) {

          //获取连接
          val jedis: Jedis = RedisUtil.getJedisClient

          //导入隐式转换
          import scala.collection.JavaConversions._

          if (jedis.exists(redisKey)) {
            jedis.del(redisKey)
          }

          //批量插入数据
          jedis.hmset(redisKey, items.toMap)

          //关闭连接
          jedis.close()

        }
      }
    })

  }

}
