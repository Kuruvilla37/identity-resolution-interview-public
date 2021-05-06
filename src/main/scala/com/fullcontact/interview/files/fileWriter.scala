package com.fullcontact.interview.files

import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.apache.spark.SparkException


object fileWriter {

  @throws(classOf[AnalysisException])
  @throws(classOf[SparkException])
  @throws(classOf[Exception])
  def fileWriter(spark: SparkSession, dataFrame: DataFrame, file: String): Unit = {

    val writeDf = dataFrame.write.format("text").save(file)

  }

}
