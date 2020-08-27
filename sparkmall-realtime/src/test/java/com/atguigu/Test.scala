package com.atguigu

import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

/**
 *
 * @auther hage
 * @creat 2020-08-25 18:31
 */
class Test {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RealTimeAPP")

    val ssc = new StreamingContext(sparkConf, Seconds(3))

    ssc.sparkContext.setCheckpointDir("./ch1")

    val topic = "test"

    val kafkaDStram: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaDStream(ssc, Array(topic))

    val value: DStream[String] = kafkaDStram.map(record => record.value())

        value.print(100)


    ssc.start()
    ssc.awaitTermination()



  }

}
