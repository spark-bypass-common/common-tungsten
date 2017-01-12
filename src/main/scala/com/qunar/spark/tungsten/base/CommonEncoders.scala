package com.qunar.spark.tungsten.base

import org.apache.spark.sql.{Encoder, Encoders, SQLContext, SQLImplicits}

import scala.reflect.ClassTag

/**
  * 针对[[SQLImplicits]]拓展的通用Encoders
  * 支持 primitive types, case classes, tuples 以及 其他复杂类型
  */
object CommonEncoders extends SQLImplicits {

  override protected def _sqlContext: SQLContext = SparkSessions.getSparkSession.sqlContext

  implicit def tuple2[A1, A2](implicit e1: Encoder[A1], e2: Encoder[A2]): Encoder[(A1, A2)] = Encoders.tuple[A1, A2](e1, e2)

  implicit def tuple3[A1, A2, A3](implicit e1: Encoder[A1], e2: Encoder[A2], e3: Encoder[A3]): Encoder[(A1, A2, A3)] = Encoders.tuple[A1, A2, A3](e1, e2, e3)

  // 针对复杂数据类型的Encoder:使用Encoders.kryo
  implicit def complex[A](implicit classTag: ClassTag[A]): Encoder[A] = Encoders.kryo[A](classTag)

}
