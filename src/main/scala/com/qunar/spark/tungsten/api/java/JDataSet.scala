package com.qunar.spark.tungsten.api.java

import com.qunar.spark.tungsten.base.{CommonEncoders, CoreJoinOperators}
import com.qunar.spark.tungsten.base.TypeConverter._
import org.apache.spark.api.java.function.{FilterFunction, FlatMapFunction, MapFunction, MapPartitionsFunction}
import org.apache.spark.sql.Dataset

import scala.language.implicitConversions

/**
  * [[com.qunar.spark.tungsten.api.DataSet]]针对java的定制api
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
    * 尽管[[Dataset]]提供了[[Dataset.join]]算子,但是对于被kryo编码的对象,则无法使用
    * 这里我们重新封装了[[org.apache.spark.rdd.RDD]]时代经典的[[leftOuterJoin]]算子,
    * 方便业务线使用.
    *
    * @param genJoinKey 数据集记录生成key的函数
    */
  def leftOuterJoin[K](anotherJDataSet: JDataSet[T], genJoinKey: MapFunction[T, K]): JDataSet[(T, T)] = {
    val dataset = CoreJoinOperators.outerJoin(innerDataset, anotherJDataSet.innerDataset, genJoinKey)
    new JDataSet[(T, T)](dataset)
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
  def cogroup[K](anotherJDataSet: JDataSet[T], genJoinKey: MapFunction[T, K]): JDataSet[(Seq[T], Seq[T])] = {
    val dataset = CoreJoinOperators.cogroup(innerDataset, anotherJDataSet.innerDataset, genJoinKey)
    new JDataSet[(Seq[T], Seq[T])](dataset)
  }

  /**
    * 将[[MapFunction]]隐式转换为函数对象[[T => K]]
    * 用于无缝调用[[CoreJoinOperators]]的各种带函数式参数的方法
    */
  implicit private def commandToFunctional[K](genJoinKey: MapFunction[T, K]): T => K = {
    def func(value: T): K = genJoinKey.call(value)
    func _
  }

}
