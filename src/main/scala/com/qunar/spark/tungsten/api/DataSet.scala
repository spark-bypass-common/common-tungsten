package com.qunar.spark.tungsten.api

import com.qunar.spark.tungsten.base.CommonEncoders._
import org.apache.spark.api.java.function.{FilterFunction, FlatMapFunction, MapFunction, MapPartitionsFunction}
import org.apache.spark.sql.Dataset

import scala.reflect.runtime.universe.TypeTag

/**
  * 针对[[org.apache.spark.sql.Dataset]]拓展的api
  * 作为一种代理模式,[[DataSet]]对[[Dataset]]的功能包装在于:
  * 平滑透明地生成钨丝编码[[org.apache.spark.sql.Encoder]]
  * <p/>
  * NOTICE: 为保护核心功能,此类只能由[[com.qunar.spark.tungsten.api.DataSets]]创建
  */
class DataSet[T: TypeTag] private[tungsten](private val innerDataset: Dataset[T]) extends Serializable {

  /* 函数式算子 */

  def filter(func: T => Boolean): DataSet[T] = {
    val newDataset = innerDataset.filter(func)
    DataSets.createFromDataset(newDataset)
  }

  def map[U: TypeTag](func: T => U): DataSet[U] = {
    val newDataset = innerDataset.map(func)
    DataSets.createFromDataset(newDataset)
  }

  def mapPartitions[U: TypeTag](func: Iterator[T] => Iterator[U]): DataSet[U] = {
    val newDataset = innerDataset.mapPartitions(func)
    DataSets.createFromDataset(newDataset)
  }

  def flatMap[U: TypeTag](func: T => TraversableOnce[U]): DataSet[U] = {
    val newDataset = innerDataset.flatMap(func)
    DataSets.createFromDataset(newDataset)
  }

  /* 命令式算子 */

  def filter(func: FilterFunction[T]): DataSet[T] = {
    null
  }

  def map[U](func: MapFunction[T, U]): DataSet[U] = {
    null
  }

  def mapPartitions[U](func: MapPartitionsFunction[T, U]): DataSet[T] = {
    null
  }

  def flatMap[U](func: FlatMapFunction[T, U]): DataSet[U] = {
    null
  }

  /* join相关的连接算子 */

  def leftOuterJoin[K: TypeTag](anotherDataSet: DataSet[T], joinKey: T => K): DataSet[(T, T)] = {
    val namedNewDataset = innerDataset.map(record => (joinKey(record), record))
      .toDF("_1", "_2").as[(K, T)]
    val namedOldDataset = anotherDataSet.innerDataset.map(record => (joinKey(record), record))
      .toDF("_1", "_2").as[(K, T)]
    val newDataset = namedNewDataset.joinWith(namedOldDataset, namedNewDataset("_1") === namedOldDataset("_2"), "left_outer")
      .map(record => (record._1._2, record._2._2))

    DataSets.createFromDataset(newDataset)
  }

  /**
    * [[Dataset]]本身并未提供类似于[[org.apache.spark.rdd.PairRDDFunctions.cogroup]]的算子,
    * 但是这个算子又有着非常重要的作用,在很多场景下无法用[[Dataset.join]]替代.
    * 这里使用[[Dataset]]的其他算子的一些组合,间接达到模仿cogroup算子输入输出特性的目的.
    */
  def cogroup[K: TypeTag](anotherDataset: DataSet[T], joinKey: T => K): DataSet[(Seq[T], Seq[T])] = {
    // 先预处理innerDataset
    val thisKeyValueSet = innerDataset.groupByKey(data => joinKey(data))
    val thisCogroupSet = thisKeyValueSet.mapGroups((key, dataIter) => {
      val builder = Seq.newBuilder[T]
      for (data <- dataIter) {
        builder += data
      }
      (key, builder.result)
    }).toDF("_1", "_2").as[(K, Seq[T])]

    // 再预处理anotherDataset
    val anotherKeyValueSet = anotherDataset.innerDataset.groupByKey(data => joinKey(data))
    val anotherCogroupSet = anotherKeyValueSet.mapGroups((key, dataIter) => {
      val builder = Seq.newBuilder[T]
      for (data <- dataIter) {
        builder += data
      }
      (key, builder.result)
    }).toDF("_1", "_2").as[(K, Seq[T])]

    val resultDataFrame = thisCogroupSet.join(anotherCogroupSet, thisCogroupSet("_1") === anotherCogroupSet("_1"), "outer")
    val newDataset = resultDataFrame.as[(Seq[T], Seq[T])]

    DataSets.createFromDataset(newDataset)
  }

}
