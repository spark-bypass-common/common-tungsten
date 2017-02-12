package com.qunar.spark.tungsten.base

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
  * scala中: [[scala.reflect.api.TypeTags.TypeTag]],[[scala.reflect.ClassTag]],[[Class]]等之间的转换
  * 供scala api 或 java api 使用
  */
object TypeConverter extends Serializable {

  /**
    * 从[[TypeTag]]中获取对应的[[Class]]
    */
  implicit def typeTagToClass[T: TypeTag]: Class[T] = {
    typeTag[T].mirror.runtimeClass(typeTag[T].tpe.typeSymbol.asClass).asInstanceOf[Class[T]]
  }

  /**
    * 将[[TypeTag]]转为[[ClassTag]]
    */
  implicit def typeTagToClassTag[T: TypeTag]: ClassTag[T] = {
    ClassTag[T](typeTag[T].mirror.runtimeClass(typeTag[T].tpe))
  }

  /**
    * 从[[Class]]中获取对应的[[TypeTag]]
    */
  implicit def classToTypeTag[T]: TypeTag[T] = {
    typeTag[T]
  }

}
