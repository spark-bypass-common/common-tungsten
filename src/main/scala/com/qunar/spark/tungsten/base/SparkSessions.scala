package com.qunar.spark.tungsten.base

import org.apache.spark.sql.SparkSession

/**
  * 获取全局的[[SparkSession]],以供隐式转换并匹配[[org.apache.spark.sql.Encoder]]
  */
object SparkSessions {

  // 只是为了获取全局session,并不需要配置各种参数,此参数应该在具体应用中被初始化完毕
  private val session = SparkSession.builder.getOrCreate

  def getSparkSession: SparkSession = session

}
