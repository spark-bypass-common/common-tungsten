package com.qunar.spark.tungsten.api

import com.hadoop.mapreduce.LzoTextInputFormat
import com.qunar.spark.base.json.JsonMapper
import com.qunar.spark.tungsten.base.SparkSessions
import com.qunar.spark.tungsten.base.CommonEncoders._
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.sql.Dataset

import scala.reflect.runtime.universe.TypeTag

/**
  * 生成与使用[[DataSet]]的通用scala入口类
  */
object DataSets extends Serializable {

  private val sparkSession = SparkSessions.getSparkSession

  /**
    * 给定一个[[Dataset]],创建对应的[[DataSet]]
    */
  private[tungsten] def createFromDataset[T: TypeTag](dataset: Dataset[T]): DataSet[T] = {
    new DataSet[T](dataset)
  }

  /**
    * 读出原始内容(text或gzip)并转成[[DataSet[String] ]]
    */
  def readTextFromHdfs(path: String): DataSet[String] = {
    val dataset = sparkSession.read
      .text(path)
      .as[String]

    createFromDataset(dataset)
  }

  /**
    * 读出原始内容(text或gzip)并转换成[[DataSet[T] ]]
    */
  def readTextFromHdfs[T: TypeTag](path: String, convert: (String) => T): DataSet[T] = {
    val dataset = sparkSession.read
      .text(path)
      .as[String]
      .map(convert)

    createFromDataset(dataset)
  }

  /**
    * 读出LZO压缩文件并转换成[[DataSet[String] ]]
    */
  def readLzoFromHdfs(path: String): DataSet[String] = {
    val dataset = sparkSession.sparkContext
      .newAPIHadoopFile[LongWritable, Text, LzoTextInputFormat](path)
      .values
      .map(_.toString)
      .toDS

    createFromDataset(dataset)
  }

  /**
    * 读出LZO压缩文件并转换成[[DataSet[T] ]]
    */
  def readLzoFromHdfs[T: TypeTag](path: String, convert: (String) => T): DataSet[T] = {
    val dataset = sparkSession.sparkContext
      .newAPIHadoopFile[LongWritable, Text, LzoTextInputFormat](path)
      .values
      .map(_.toString)
      .toDS
      .map(convert)

    createFromDataset(dataset)
  }

  /**
    * 将[[DataSet[T] ]]写入hdfs
    */
  def writeToHdfs[T](content: DataSet[T], path: String): Unit = {
    content.getInnerDataset.map(record => JsonMapper.writeValueAsString(record)).write.save(path)
  }

}
