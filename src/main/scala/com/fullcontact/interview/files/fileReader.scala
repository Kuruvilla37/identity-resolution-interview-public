package com.fullcontact.interview.files

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.typesafe.config.Config
import org.apache.spark.SparkException



object fileReader {

  @throws(classOf[AnalysisException])
  @throws(classOf[SparkException])
  @throws(classOf[Exception])
  def fileReader(spark: SparkSession, schema: StructType, recSchema: StructType, config: Config): (DataFrame, DataFrame) = {

    val QueriesDf = spark.read.option("header", "false")
      .schema(schema)
      .csv(config.getString("Paths.Queries_path"))

    val recordsDf = spark.read.option("header","false")
      .schema(recSchema)
      .text(config.getString("Paths.Record_path"))
      .select(split(col("records"), " ").alias("records"))

    (QueriesDf,recordsDf)

  }



}
