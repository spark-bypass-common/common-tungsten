package com.qunar.test

import com.qunar.spark.tungsten.api.DataSets
import org.junit.Test

class ScalaTest {

  @Test
  def test(): Unit = {
    DataSets.readLzoFromHdfs("sss", str => str)
  }

}
