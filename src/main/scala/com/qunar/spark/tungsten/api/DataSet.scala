package com.qunar.spark.tungsten.api

import com.qunar.spark.tungsten.base.CommonEncoders._
import org.apache.spark.api.java.function.{FilterFunction, FlatMapFunction, MapFunction, MapPartitionsFunction}
import org.apache.spark.sql.Dataset

import scala.reflect.runtime.universe.TypeTag


/**
  * 针对[[org.apache.spark.sql.Dataset]]拓展的api
  * 作为一种代理模式,[[DataSet]]对[[Dataset]]的功能包装在于:  平滑透明地生成钨丝编码[[org.apache.spark.sql.Encoder]]
  * <p/>
  * NOTICE: 为保护核心功能,此类只能由[[com.qunar.spark.tungsten.api.DataSets]]创建
  */
class DataSet[T] private[tungsten](private val innerDataset: Dataset[T]) extends Serializable {

  /* 函数式算子 */

  def filter(func: T => Boolean): DataSet[T] = {
    val newDataset = innerDataset.filter(func)
    DataSets.createFromDataset(newDataset)
  }

  def map[U: TypeTag](func: T => U): DataSet[U] = {
    val newDataset = innerDataset.map(func)
    DataSets.createFromDataset(newDataset)
  }

  def mapPartitions[U: TypeTag](func: Iterator[T] => Iterator[U]): DataSet[T] = {
    null
  }

  def flatMap[U](func: T => TraversableOnce[U]): DataSet[U] = {
    null
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

  /* join相关算子 */

  def leftOuterJoin(anotherDataSet: DataSet[T]): DataSet[(T, T)] = {
    null
  }

  def cogroup(anotherDataset: DataSet[T]): DataSet[(String, (Seq[T], Seq[T]))] = {
    null
  }

}
