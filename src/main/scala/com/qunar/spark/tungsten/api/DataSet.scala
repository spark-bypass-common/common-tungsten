package com.qunar.spark.tungsten.api

import com.qunar.spark.tungsten.base.CommonEncoders._
import org.apache.spark.sql.Dataset

import scala.reflect.runtime.universe.TypeTag

/**
  * 针对[[org.apache.spark.sql.Dataset]]拓展的api
  * 作为一种代理模式,那么[[DataSet]]对[[Dataset]]的功能包装在于:
  * 1. 平滑透明地生成钨丝编码[[org.apache.spark.sql.Encoder]]
  * 2. 封装[[Dataset]]所没有提供的[[leftOuterJoin]]方法与[[cogroup]]方法,方便业务线使用
  * <p/>
  * NOTICE: 为保护核心功能,此类只能由[[com.qunar.spark.tungsten]]包内的类构造
  */
class DataSet[T: TypeTag] private[tungsten](private val innerDataset: Dataset[T]) extends Serializable {

  /**
    * 仅对[[com.qunar.spark.tungsten]]内部的类提供支持,保护核心功能
    */
  private[tungsten] def getInnerDataset: Dataset[T] = innerDataset

  /* 函数式算子 针对scala api */

  def filter(func: T => Boolean): DataSet[T] = {
    val newDataset = innerDataset.filter(func)
    new DataSet[T](newDataset)
  }

  def map[U: TypeTag](func: T => U): DataSet[U] = {
    val newDataset = innerDataset.map(func)
    new DataSet[U](newDataset)
  }

  def mapPartitions[U: TypeTag](func: Iterator[T] => Iterator[U]): DataSet[U] = {
    val newDataset = innerDataset.mapPartitions(func)
    new DataSet[U](newDataset)
  }

  def flatMap[U: TypeTag](func: T => TraversableOnce[U]): DataSet[U] = {
    val newDataset = innerDataset.flatMap(func)
    new DataSet[U](newDataset)
  }

  /* join相关的连接算子 */

  /**
    * ''左外连接算子''
    * </p>
    * 尽管[[Dataset]]提供了[[Dataset.join]]算子,但是对于被kryo编码的对象,则无法使用
    * 这里我们重新封装了[[org.apache.spark.rdd.RDD]]时代经典的[[leftOuterJoin]]算子,
    * 方便业务线使用.
    *
    * @param genJoinKey 数据集记录生成key的函数
    */
  def leftOuterJoin[K: TypeTag](anotherDataSet: DataSet[T], genJoinKey: T => K): DataSet[(T, T)] = {
    val namedNewDataset = innerDataset.map(record => (genJoinKey(record), record))
      .toDF("_1", "_2").as[(K, T)]
    val namedOldDataset = anotherDataSet.innerDataset.map(record => (genJoinKey(record), record))
      .toDF("_1", "_2").as[(K, T)]
    val newDataset = namedNewDataset.joinWith(namedOldDataset, namedNewDataset("_1") === namedOldDataset("_1"), "left_outer")
      .map(record => (record._1._2, record._2._2))

    DataSets.createFromDataset(newDataset)
  }

  /**
    * ''cogroup算子''
    * </p>
    * [[Dataset]]本身并未提供类似于[[org.apache.spark.rdd.PairRDDFunctions.cogroup]]的算子,
    * 但是这个算子又有着非常重要的作用,在很多场景下无法用[[Dataset.join]]替代.
    * 这里使用[[Dataset]]的其他算子的一些组合,间接达到模仿cogroup算子输入输出特性的目的.
    *
    * @param genJoinKey 数据集记录生成key的函数
    */
  def cogroup[K: TypeTag](anotherDataset: DataSet[T], genJoinKey: T => K): DataSet[(Seq[T], Seq[T])] = {
    // 先预处理innerDataset
    val thisKeyValueSet = innerDataset.groupByKey(data => genJoinKey(data))
    val thisCogroupSet = thisKeyValueSet.mapGroups((key, dataIter) => {
      val builder = Seq.newBuilder[T]
      for (data <- dataIter) {
        builder += data
      }
      (key, builder.result)
    }).toDF("_1", "_2").as[(K, Seq[T])]

    // 再预处理anotherDataset
    val anotherKeyValueSet = anotherDataset.innerDataset.groupByKey(data => genJoinKey(data))
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
