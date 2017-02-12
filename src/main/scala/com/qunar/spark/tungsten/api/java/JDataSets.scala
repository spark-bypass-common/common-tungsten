package com.qunar.spark.tungsten.api.java

import com.hadoop.mapreduce.LzoTextInputFormat
import com.qunar.spark.base.json.JsonMapper
import com.qunar.spark.tungsten.api.DataSet
import com.qunar.spark.tungsten.base.CommonEncoders._
import com.qunar.spark.tungsten.base.{CommonEncoders, SparkSessions, TypeConverter}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.Dataset
import scala.language.implicitConversions

/**
  * 生成与使用[[JDataSet]]的通用scala入口类
  */
object JDataSets extends Serializable {

  private val sparkSession = SparkSessions.getSparkSession

  /**
    * 给定一个[[Dataset]],创建对应的[[DataSet]]
    */
  private[tungsten] def createFromDataset[T](dataset: Dataset[T]): JDataSet[T] = {
    new JDataSet[T](dataset)
  }

  /**
    * 读出原始内容(text或gzip)并转成[[Dataset[String] ]]
    */
  def readTextFromHdfs(path: String): JDataSet[String] = {
    val dataset = sparkSession.read
      .text(path)
      .as[String]

    createFromDataset(dataset)
  }

  /**
    * 读出原始内容(text或gzip)并转换成[[Dataset[T] ]]
    */
  def readTextFromHdfs[T](path: String, convert: MapFunction[String, T]): JDataSet[T] = {
    val dataset = sparkSession.read
      .text(path)
      .as[String]
      .map(convert, CommonEncoders.encoderForSingle(TypeConverter.classToTypeTag[T]))

    createFromDataset(dataset)
  }

  /**
    * 读出LZO压缩文件并转换成[[Dataset[String] ]]
    */
  def readLzoFromHdfs(path: String): JDataSet[String] = {
    val dataset = sparkSession.sparkContext
      .newAPIHadoopFile[LongWritable, Text, LzoTextInputFormat](path)
      .map(record => record._2.toString)
      .toDS

    createFromDataset(dataset)
  }

  /**
    * 读出LZO压缩文件并转换成[[Dataset[T] ]]
    */
  def readLzoFromHdfs[T](path: String, convert: MapFunction[String, T]): JDataSet[T] = {
    val dataset = sparkSession.sparkContext
      .newAPIHadoopFile[LongWritable, Text, LzoTextInputFormat](path)
      .map(record => record._2.toString)
      .toDS
      .map(convert, CommonEncoders.encoderForSingle(TypeConverter.classToTypeTag[T]))

    createFromDataset(dataset)
  }

  /**
    * 写入hdfs
    */
  def writeToHdfs[T](content: DataSet[T], path: String): Unit = {
    content.map(record => JsonMapper.writeValueAsString(record)).getInnerDataset.write.save(path)
  }

}
