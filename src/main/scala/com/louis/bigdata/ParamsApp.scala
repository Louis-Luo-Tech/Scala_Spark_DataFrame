package com.louis.bigdata

import com.typesafe.config.ConfigFactory

object ParamsApp {
  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load()
    val url = config.getString("db.default.url")
    println(url)
  }

}
