package com.atguigu.app

import com.atguigu.bean.AdsLog
import com.atguigu.handler.{BlackListHandler, DateAreaAdsTop3CountHandler, DateAreaCityAdsCountHandler, LastHourAdsClickCountHandler}
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 *
 * @auther hage
 * @creat 2020-08-25 17:13
 */
object RealTimeApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("RealTimeAPP")

    val ssc = new StreamingContext(sparkConf, Seconds(3))

    ssc.sparkContext.setCheckpointDir("./ch1")

    val topic = "ads_log"

    println(s"Driver=======${Thread.currentThread().getName}")

    val kafkaDStram: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaDStream(ssc, Array(topic))

    val adsLogDStream: DStream[AdsLog] = kafkaDStram.map(record => {

      val splits: Array[String] = record.value().split(" ")
      AdsLog(splits(0).toLong, splits(1), splits(2), splits(3), splits(4))
    })
    adsLogDStream.print()

    //6.查询黑名单进行数据过滤
    val filterdDStream: DStream[AdsLog] = BlackListHandler.filterDataByBlackList(ssc.sparkContext, adsLogDStream)

    filterdDStream.cache()
    //需求六统计每天每个地区每个城市每个广告的点击次数
    val dateAreaCityAdsToCount: DStream[(String, Long)] = DateAreaCityAdsCountHandler.getDateAreaCityAdsCount(filterdDStream)

//将每天每个地区每个城市每个广告的点击次数写入Redis
    DateAreaCityAdsCountHandler.saveDataToRedis(dateAreaCityAdsToCount)

    //需求七：每天各地区 top3 热门广告存入Redis
    DateAreaAdsTop3CountHandler.saveDateAreaAdsTop3CountToRedis(dateAreaCityAdsToCount)

    //需求八：统计各广告最近 1 小时内的点击量趋势：各广告最近 1 小时内各分钟的点击量并存入Redis
    LastHourAdsClickCountHandler.saveLastHourClickCountToRedis(filterdDStream)


    //7.校验数据是否需要加入黑名单，如果超过100次，则加入黑名单
    BlackListHandler.checkUserToBlackList(filterdDStream)
    adsLogDStream.print()

    ssc.start()
    ssc.awaitTermination()



  }

}
