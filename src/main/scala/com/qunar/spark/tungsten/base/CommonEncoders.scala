package com.qunar.spark.tungsten.base

import scala.reflect.runtime.universe._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{ColumnName, _}

import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
  * 针对[[SQLImplicits]]改造并拓展的通用钨丝编码器
  * 支持 primitive types, case classes, tuples 以及 其他复杂类型
  */
object CommonEncoders {

  // case class
  private val CLASS_OF_PRODUCT = classOf[Product]
  // scala primitive types
  private val CLASS_OF_INT = classOf[Int]
  private val CLASS_OF_LONG = classOf[Long]
  private val CLASS_OF_DOUBLE = classOf[Double]
  private val CLASS_OF_FLOAT = classOf[Float]
  private val CLASS_OF_SHORT = classOf[Short]
  private val CLASS_OF_BYTE = classOf[Byte]
  private val CLASS_OF_BOOLEAN = classOf[Boolean]
  // java primitive types
  private val CLASS_OF_JAVA_INT = classOf[java.lang.Integer]
  private val CLASS_OF_JAVA_LONG = classOf[java.lang.Long]
  private val CLASS_OF_JAVA_DOUBLE = classOf[java.lang.Double]
  private val CLASS_OF_JAVA_FLOAT = classOf[java.lang.Float]
  private val CLASS_OF_JAVA_SHORT = classOf[java.lang.Short]
  private val CLASS_OF_JAVA_BYTE = classOf[java.lang.Byte]
  private val CLASS_OF_JAVA_BOOLEAN = classOf[java.lang.Boolean]
  // Seq(考虑到encoder方法的内部逻辑中需要容器支持协变特性,故这里只能支持Seq而不能支持Array)
  private val CLASS_OF_PRODUCT_SEQ = classOf[Seq[Product]]
  private val CLASS_OF_INT_SEQ = classOf[Seq[Int]]
  private val CLASS_OF_LONG_SEQ = classOf[Seq[Long]]


  /**
    * 通用的隐式编码器:接收一切类型并在内部逻辑中判断采用最合适的编码器
    * <p/>
    * NOTICE: 与[[SQLImplicits]]针对不同类型编写不同的隐式编码器的策略不同,
    * 本方法接收一切类型并在内部判断所传类型的具体所属,再路由到不同的实际编
    * 码器.
    * 这样处理可以使外部在使用[[Dataset]]时无需关注自己所传入的类型[[A]],对
    * 任何类型[[A]]都能适配.
    */
  implicit def encoder[A: TypeTag]: Encoder[A] = {
    // 获取A所对应的Class
    val clazz = typeTagToClass

    if (CLASS_OF_PRODUCT.isAssignableFrom(clazz)) {
      ExpressionEncoder[A]()
    } else if (CLASS_OF_INT.isAssignableFrom(clazz)) {
      ExpressionEncoder[A]()
    } else if (CLASS_OF_JAVA_INT.isAssignableFrom(clazz)) {
      ExpressionEncoder[A]()
    } else if (CLASS_OF_PRODUCT_SEQ.isAssignableFrom(clazz)) {
      ExpressionEncoder[A]()
    } else {
      // 如果没有Encoder能匹配上,那么Encoders.kryo是最后的选择
      Encoders.kryo[A]
    }
  }

  private def typeTagToClass[T: TypeTag]: Class[T] = {
    typeTag[T].mirror.runtimeClass(typeTag[T].tpe.typeSymbol.asClass).asInstanceOf[Class[T]]
  }

  implicit def typeTagToClassTag[T: TypeTag]: ClassTag[T] = {
    ClassTag[T](typeTag[T].mirror.runtimeClass(typeTag[T].tpe))
  }

  /**
    * 以下两个隐式方法是为了解决[[Tuple2]]与[[Tuple3]]的钨丝编码问题
    */
  implicit def tuple2[A1, A2](implicit e1: Encoder[A1], e2: Encoder[A2]
                             ): Encoder[(A1, A2)] = Encoders.tuple[A1, A2](e1, e2)

  implicit def tuple3[A1, A2, A3](implicit e1: Encoder[A1], e2: Encoder[A2], e3: Encoder[A3]
                                 ): Encoder[(A1, A2, A3)] = Encoders.tuple[A1, A2, A3](e1, e2, e3)

  /**
    * 以下四个隐式方法/类均 copy from [[SQLImplicits]]
    */
  private def _sqlContext: SQLContext = SparkSessions.getSparkSession.sqlContext

  implicit class StringToColumn(val sc: StringContext) {
    def $(args: Any*): ColumnName = {
      new ColumnName(sc.s(args: _*))
    }
  }

  implicit def rddToDatasetHolder[A: Encoder](rdd: RDD[A]): DatasetHolder[A] = {
    DatasetHolder(_sqlContext.createDataset(rdd))
  }

  implicit def localSeqToDatasetHolder[A: Encoder](s: Seq[A]): DatasetHolder[A] = {
    DatasetHolder(_sqlContext.createDataset(s))
  }

  implicit def symbolToColumn(s: scala.Symbol): ColumnName = new ColumnName(s.name)

}
