package com.atguigu.bean

/**
 *
 * @auther hage
 * @creat 2020-08-25 0:03
 */
case class CityRatio(cityName: String, ratio: Double) {

  override def toString: String = s"$cityName:$ratio%"
}
