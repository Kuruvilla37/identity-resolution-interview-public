package com.fullcontact.interview.sparkconfig

import com.typesafe.config.Config
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.{SparkConf, SparkException}

object SparkHelper {

  @throws(classOf[AnalysisException])
  @throws(classOf[SparkException])
  @throws(classOf[Exception])
  def sparkEntry(config: Config): SparkSession = {

    val conf = new SparkConf()
      .setMaster(config.getString("spark.master"))
      .setAppName(config.getString("spark.appName"))

    val spark = SparkSession.builder().config(conf).getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    spark

  }

}
