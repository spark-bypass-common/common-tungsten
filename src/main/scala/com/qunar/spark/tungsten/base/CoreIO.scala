package com.qunar.spark.tungsten.base

import com.hadoop.mapreduce.LzoTextInputFormat
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.sql.Dataset
import com.qunar.spark.tungsten.base.CommonEncoders._

/**
  */
object CoreIO {

  private val sparkSession = SparkSessions.getSparkSession

  def read(path: String): Dataset[String] = {
    sparkSession.read
      .text(path)
      .as[String]
  }

  def readLZO(path: String): Dataset[String] = {
    sparkSession.sparkContext
      .newAPIHadoopFile[LongWritable, Text, LzoTextInputFormat](path)
      .map(record => record._2.toString)
      .toDS
  }

}
