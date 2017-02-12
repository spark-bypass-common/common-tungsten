package com.qunar.spark.tungsten.api.java

import com.qunar.spark.tungsten.base.{CommonEncoders, CoreJoinOperators}
import com.qunar.spark.tungsten.base.TypeConverter._
import org.apache.spark.api.java.function.{FilterFunction, FlatMapFunction, MapFunction, MapPartitionsFunction}
import org.apache.spark.sql.Dataset

import scala.language.implicitConversions

/**
  * 区别于[[com.qunar.spark.tungsten.api.DataSet]],针对java的定制api
  * </p>
  * NOTICE: 为保护核心功能,此类只能由[[com.qunar.spark.tungsten]]包内的类构造
  */
class JDataSet[T] private[tungsten](private val innerDataset: Dataset[T]) extends Serializable {

  /**
    * 仅对[[com.qunar.spark.tungsten]]内部的类提供支持,保护核心功能
    */
  private[tungsten] def getInnerDataset: Dataset[T] = innerDataset

  /* 命令式算子 针对java api */

  /**
    * 以下四个常用的算子专为 java api 设计,所有的方法签名都屏蔽了
    * [[scala.reflect.api.TypeTags.TypeTag]]与Spark SQL所需要的
    * [[org.apache.spark.sql.Encoder]],而将实现细节封装在方法内部,
    * 以求给java开发者提供一组简洁的接口.
    */

  def filter(func: FilterFunction[T]): JDataSet[T] = {
    val newDataset = innerDataset.filter(func)
    new JDataSet[T](newDataset)
  }

  def map[U](func: MapFunction[T, U]): JDataSet[U] = {
    val newDataset = innerDataset.map(func, CommonEncoders.encoderForSingle[U])
    new JDataSet[U](newDataset)
  }

  def mapPartitions[U](func: MapPartitionsFunction[T, U]): JDataSet[U] = {
    val newDataset = innerDataset.mapPartitions(func, CommonEncoders.encoderForSingle[U])
    new JDataSet[U](newDataset)
  }

  def flatMap[U](func: FlatMapFunction[T, U]): JDataSet[U] = {
    val newDataset = innerDataset.flatMap(func, CommonEncoders.encoderForSingle[U])
    new JDataSet[U](newDataset)
  }

  /* join相关的连接算子 */

  /**
    * ''左外连接算子''
    * </p>
    * 利用[[CoreJoinOperators.strongTypeJoin]]方法构造左外连接算子
    * 这里将不会提供右外连接,因为其本质上可以用左外连接实现
    *
    * @param genJoinKey 数据集记录生成key的函数
    */
  def leftOuterJoin[K](anotherJDataSet: JDataSet[T], genJoinKey: MapFunction[T, K]): JDataSet[(T, T)] = {
    val dataset = CoreJoinOperators.strongTypeJoin(innerDataset, anotherJDataSet.innerDataset, genJoinKey, "left_outer")
    new JDataSet[(T, T)](dataset)
  }

  /**
    * 同上,''内连接算子''
    */
  def innerJoin[K](anotherJDataSet: JDataSet[T], genJoinKey: MapFunction[T, K]): JDataSet[(T, T)] = {
    val dataset = CoreJoinOperators.strongTypeJoin(innerDataset, anotherJDataSet.innerDataset, genJoinKey, "inner")
    new JDataSet[(T, T)](dataset)
  }

  /**
    * 同上,''全外连接算子''
    */
  def fullOuterJoin[K](anotherJDataSet: JDataSet[T], genJoinKey: MapFunction[T, K]): JDataSet[(T, T)] = {
    val dataset = CoreJoinOperators.strongTypeJoin(innerDataset, anotherJDataSet.innerDataset, genJoinKey, "outer")
    new JDataSet[(T, T)](dataset)
  }

  /**
    * ''cogroup算子''
    * </p>
    * 对[[CoreJoinOperators.cogroup]]方法的简单封装
    *
    * @param genJoinKey 数据集记录生成key的函数
    */
  def cogroup[K](anotherJDataSet: JDataSet[T], genJoinKey: MapFunction[T, K]): JDataSet[(Seq[T], Seq[T])] = {
    val dataset = CoreJoinOperators.cogroup(innerDataset, anotherJDataSet.innerDataset, genJoinKey)
    new JDataSet[(Seq[T], Seq[T])](dataset)
  }

}
