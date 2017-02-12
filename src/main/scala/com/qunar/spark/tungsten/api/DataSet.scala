package com.qunar.spark.tungsten.api

import com.qunar.spark.tungsten.base.CommonEncoders._
import com.qunar.spark.tungsten.base.CoreJoinOperators
import org.apache.spark.sql.Dataset

import scala.reflect.runtime.universe.TypeTag

/**
  * 针对[[org.apache.spark.sql.Dataset]]拓展的api
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
    * 利用[[CoreJoinOperators.strongTypeJoin]]方法构造左外连接算子
    * 这里将不会提供右外连接,因为其本质上可以用左外连接实现
    *
    * @param genJoinKey 数据集记录生成key的函数
    */
  def leftOuterJoin[K: TypeTag](anotherDataSet: DataSet[T], genJoinKey: T => K): DataSet[(T, T)] = {
    val dataset = CoreJoinOperators.strongTypeJoin(innerDataset, anotherDataSet.innerDataset, genJoinKey, "left_outer")
    new DataSet[(T, T)](dataset)
  }

  /**
    * 同上,''内连接算子''
    */
  def innerJoin[K: TypeTag](anotherDataSet: DataSet[T], genJoinKey: T => K): DataSet[(T, T)] = {
    val dataset = CoreJoinOperators.strongTypeJoin(innerDataset, anotherDataSet.innerDataset, genJoinKey, "inner")
    new DataSet[(T, T)](dataset)
  }

  /**
    * 同上,''全外连接算子''
    */
  def fullOuterJoin[K: TypeTag](anotherDataSet: DataSet[T], genJoinKey: T => K): DataSet[(T, T)] = {
    val dataset = CoreJoinOperators.strongTypeJoin(innerDataset, anotherDataSet.innerDataset, genJoinKey, "outer")
    new DataSet[(T, T)](dataset)
  }

  /**
    * ''cogroup算子''
    * </p>
    * 对[[CoreJoinOperators.cogroup]]方法的简单封装
    *
    * @param genJoinKey 数据集记录生成key的函数
    */
  def cogroup[K: TypeTag](anotherDataSet: DataSet[T], genJoinKey: T => K): DataSet[(Seq[T], Seq[T])] = {
    val dataset = CoreJoinOperators.cogroup(innerDataset, anotherDataSet.innerDataset, genJoinKey)
    new DataSet[(Seq[T], Seq[T])](dataset)
  }

}
