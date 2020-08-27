package com.atguigu.utils

import scala.collection.mutable.ListBuffer
import scala.util.Random

case class RanOpt[T](value: T, weight: Int)

object RandomOptions {
  def apply[T](opts: RanOpt[T]*): RandomOptions[T] = {
    val randomOptions = new RandomOptions[T]()
    for (opt <- opts) {
      randomOptions.totalWeight += opt.weight
      for (i <- 1 to opt.weight) {
        randomOptions.optsBuffer += opt.value

//        println(randomOptions.totalWeight)
//        println(randomOptions.optsBuffer.tail)
      }
    }
    randomOptions
  }

  def main(args: Array[String]): Unit = {
    val value = RandomOptions(RanOpt("search", 20), RanOpt("click", 60), RanOpt("order", 6), RanOpt("pay", 4), RanOpt("quit", 10))

  }

}

class RandomOptions[T](opts: RanOpt[T]*) {

  var totalWeight = 0
  var optsBuffer = new ListBuffer[T]

  def getRandomOpt: T = {
    val randomNum: Int = new Random().nextInt(totalWeight)
    optsBuffer(randomNum)
  }
}

