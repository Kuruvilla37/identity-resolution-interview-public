package com.fullcontact.interview

import com.fullcontact.interview.files.{fileReader, fileWriter}
import com.fullcontact.interview.schema.{Record, Schema}
import com.fullcontact.interview.sparkconfig.SparkHelper

import java.io.File
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType


object RecordFinder {

  val logger = Logger.getLogger(this.getClass.getName)
  logger.setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    try{
      if (args.length < 1)

        {
          logger.info("Input parameter file is required")
          System.exit(1)
        }

      // Load the  input configuration file
      val configPath = args(0)
      val confPath = ConfigFactory.parseFile(new File(configPath))
      val config = ConfigFactory.load(confPath)

     // Creating the spark session
      val spark = SparkHelper.sparkEntry(config)
      import spark.implicits._

      // Creating the schema

      val schema = ScalaReflection.schemaFor[Schema].dataType.asInstanceOf[StructType]
      val recSchema = ScalaReflection.schemaFor[Record].dataType.asInstanceOf[StructType]

      // Read the files as spark Dataframe
      val (queriesDF,recordsDf) = fileReader.fileReader(spark,schema,recSchema,config)

      // To remove the [] from the array column and to explode the array to a string column
      val transformedDf = recordsDf.select(concat_ws(" ", col("records"))
                         .alias("records"), explode(col("records")).alias("query_id"))

      transformedDf.show(20, false)

      // To get (output1.txt - which have the duplicate records)
      val withDuplicateDF = queriesDF.join(transformedDf, "query_id")

      val aggDf = withDuplicateDF.groupBy("query_id").agg(collect_list("records").alias("records"))

      // To get the distinct values from the list

      val distnctd = udf((x: String) => x.split(" ").toSet.mkString(" "))

      val withoutDupdf = aggDf.select(col("query_id"), distnctd(concat_ws(" ", col("records"))).alias("records"))

      withDuplicateDF.show(false)
      withoutDupdf.show(false)


      val finalDuplicateDf = withDuplicateDF.select(concat(col("query_id"),lit(":"),col("records")).as("Records"))
      finalDuplicateDf.show(false)

      val finalWithoutDuplicateDf = withoutDupdf.select(concat(col("query_id"), lit(":"),col("records")).as("Records"))
      finalWithoutDuplicateDf.show(false)

      val dupeFilePath = config.getString("Paths.duplicate_file")
      val nonDupeFilePath = config.getString("Paths.nonDuplicate_file")

      val repartionedDf = finalWithoutDuplicateDf.repartition(1)

      fileWriter.fileWriter(spark,finalDuplicateDf,dupeFilePath)
      fileWriter.fileWriter(spark,repartionedDf,nonDupeFilePath)

      print("Completed!!!!")

    }

    catch {
      case ex: Exception => logger.error(ex.printStackTrace())
    }
  }
}
