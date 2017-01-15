package com.qunar.spark.tungsten.api

/**
  * 可被钨丝编码[[org.apache.spark.sql.Encoder]]的复杂对象
  * 其自身无逻辑,只是用作mark,以被[[com.qunar.spark.tungsten.base.CommonEncoders]]识别并区分于[[Product]]
  */
class TungstenableComplex extends Serializable {}
