package com.atguigu.handler

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.atguigu.bean.AdsLog
import com.atguigu.utils.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

/**
 *
 * @auther hage
 * @creat 2020-08-25 22:00
 */
object BlackListHandler {


  private val sdf = new SimpleDateFormat("yyy-MM-dd")

  private val redisKey = "date-user-ads"
  private val blackList = "blackList"

  def filterDataByBlackList(sc: SparkContext, adsLogDStream: DStream[AdsLog]): DStream[AdsLog] = {

    adsLogDStream.transform(rdd => {

      println(s"transform=======${Thread.currentThread().getName}")

      //1.获取Redis客户端
      val jedis: Jedis = RedisUtil.getJedisClient

      //2.取出黑名单
      val userids: util.Set[String] = jedis.smembers(blackList)
      println(s"--userids----------------$userids")

      //3.使用广播变量
      val userIdsBC: Broadcast[util.Set[String]] = sc.broadcast(userids)

      //4.关闭连接
      jedis.close()

      val filteredDataRDD: RDD[AdsLog] = rdd.mapPartitions(items => {

        //5.进行过滤
        items.filter(adsLog => {

//          println(s"filter===${userIdsBC.value}====${Thread.currentThread().getName}")

          val userid: String = adsLog.userid

          !userIdsBC.value.contains(userid)
        })

      })
      filteredDataRDD
    })

  }


  def filterDataByBlackList1(sc: SparkContext, adsLogDStream: DStream[AdsLog]): DStream[AdsLog] = {

    adsLogDStream.transform(rdd=>{

      println(s"transform=======${Thread.currentThread().getName}")

      val jedis: Jedis = RedisUtil.getJedisClient
      val userids: util.Set[String] = jedis.smembers(blackList)
      jedis.close()
      val userIdsBC: Broadcast[util.Set[String]] = sc.broadcast(userids)


      val filteredDataRDD: RDD[AdsLog] = rdd.mapPartitions(items => {

        items.filter(adsLog => {
          println(s"filteredDataRDD=======${Thread.currentThread().getName}")
          !userIdsBC.value.contains(adsLog.userid)
          //          !userids.contains(adsLog.userid)
          //          jedis.sismember(blackList,adsLog.userid)
        })
      })
      filteredDataRDD
    }
    )


  }


  def checkUserToBlackList(adsLogDStream: DStream[AdsLog]) = {

    val dataUserAdsOne: DStream[(String, Long)] = adsLogDStream.map(adsLog => {
      val dataStr: String = sdf.format(new Date(adsLog.timestamp))

      val key = s"$dataStr:${adsLog.userid}:${adsLog.adid}"

      (key, 1L)
    })

    val dataUserAdsSum: DStream[(String, Long)] = dataUserAdsOne.reduceByKey(_ + _)

    dataUserAdsSum.foreachRDD(rdd=>{
//      println(s"foreachRDD=======${Thread.currentThread().getName}")

      rdd.foreachPartition(items=>{

        val jedis: Jedis = RedisUtil.getJedisClient

        items.foreach{case (key,count)=>{

//          println(s"items=======${Thread.currentThread().getName}")
          jedis.hincrBy(redisKey,key,count)

          if(jedis.hget(redisKey,key).toLong>=100L){

            val userid: String = key.split(":")(1)
            jedis.sadd(blackList,userid)
          }
        }

        }
        jedis.close()
      })


    })
  }



}
