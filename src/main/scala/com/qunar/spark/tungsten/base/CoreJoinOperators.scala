package com.qunar.spark.tungsten.base

import org.apache.spark.sql.Dataset
import com.qunar.spark.tungsten.base.CommonEncoders._
import scala.reflect.runtime.universe.TypeTag

/**
  * 核心join相关算子的api
  * </p>
  * 尽管[[Dataset]]提供了[[Dataset.join]]算子,但是对于被kryo编码的复杂对象,由于二进制编码
  * 本身的局限性,却无法使用Spark SQL原生的join算子.
  * 本类的作用就是提供针对以上局限性的间接解决方案:对象拆分与二元组化.
  */
object CoreJoinOperators {

  /**
    * ''外连接算子''
    * </p>
    * 这里我们重新封装了[[org.apache.spark.rdd.RDD]]时代经典的XXOuterJoin算子,
    * 方便业务线使用.
    *
    * @param genJoinKey 数据集记录生成key的函数
    */
  def outerJoin[T: TypeTag, K: TypeTag](leftDataset: Dataset[T], rightDataset: Dataset[T], genJoinKey: T => K): Dataset[(T, T)] = {
    val namedLeftDataset = leftDataset.map(record => (genJoinKey(record), record))
      .toDF("_1", "_2").as[(K, T)]
    val namedRightDataset = rightDataset.map(record => (genJoinKey(record), record))
      .toDF("_1", "_2").as[(K, T)]
    namedLeftDataset.joinWith(namedRightDataset, namedLeftDataset("_1") === namedRightDataset("_1"), "left_outer")
      .map(record => (record._1._2, record._2._2))
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
  def cogroup[T: TypeTag, K: TypeTag](leftDataset: Dataset[T], rightDataset: Dataset[T], genJoinKey: T => K): Dataset[(Seq[T], Seq[T])] = {
    // 先预处理leftDataset
    val thisKeyValueSet = leftDataset.groupByKey(data => genJoinKey(data))
    val thisCogroupSet = thisKeyValueSet.mapGroups((key, dataIter) => {
      val builder = Seq.newBuilder[T]
      for (data <- dataIter) {
        builder += data
      }
      (key, builder.result)
    }).toDF("_1", "_2").as[(K, Seq[T])]

    // 再预处理rightDataset
    val anotherKeyValueSet = rightDataset.groupByKey(data => genJoinKey(data))
    val anotherCogroupSet = anotherKeyValueSet.mapGroups((key, dataIter) => {
      val builder = Seq.newBuilder[T]
      for (data <- dataIter) {
        builder += data
      }
      (key, builder.result)
    }).toDF("_1", "_2").as[(K, Seq[T])]

    val resultDataFrame = thisCogroupSet.join(anotherCogroupSet, thisCogroupSet("_1") === anotherCogroupSet("_1"), "outer")
    resultDataFrame.as[(Seq[T], Seq[T])]
  }

}
