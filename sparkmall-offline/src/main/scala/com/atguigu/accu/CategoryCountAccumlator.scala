package com.atguigu.accu

import org.apache.spark.util.AccumulatorV2
import scala.collection.mutable

/**
 *
 * @auther hage
 * @creat 2020-08-24 0:33
 */
class CategoryCountAccumlator extends AccumulatorV2[String,mutable.HashMap[String,Long]] {

//  private val categoryCount = new mutable.HashMap[String, Long]()
  private var categoryCountMap = new mutable.HashMap[String, Long]()

  override def isZero: Boolean = categoryCountMap.isEmpty

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
    val accumlator = new CategoryCountAccumlator
    accumlator.value ++= categoryCountMap
    accumlator

  }

  override def reset(): Unit = categoryCountMap.clear()

  override def add(v: String): Unit = {
    categoryCountMap(v) = categoryCountMap.getOrElse(v, 0L) + 1L
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {

    val value1: mutable.HashMap[String, Long] = other.value
    println(value1.take(5))

//    val categoaryMerge: mutable.HashMap[String, Long] = categoryCount.foldLeft(other.value) {
//      case (otherMap, (category, count)) =>
//        categoryCount(category) = count + otherMap.getOrElse(category, 0L)
//        categoryCount
//    }

    val sessionMapOther: mutable.HashMap[String, Long] = other.value

    val mergedSessionMap: mutable.HashMap[String, Long] = this.categoryCountMap.foldLeft(sessionMapOther) {
      case (sessionOther: mutable.HashMap[String, Long], (key, count)) =>
      sessionOther(key) = sessionMapOther.getOrElse(key, 0L) + count
      sessionOther
    }
    this.categoryCountMap = mergedSessionMap
    println("-----")
    println(categoryCountMap.take(5))
  }
//  def mergeHashMap(sessionOther1: mutable.HashMap[String, Long], sessionOther2: mutable.HashMap[String, Long]): Unit ={
//    sessionOther1.foldLeft(sessionOther2)
//
//  }

  override def value: mutable.HashMap[String, Long] = categoryCountMap
}
