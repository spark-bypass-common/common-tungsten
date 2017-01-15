package com.qunar.test

import org.junit.Test

class ScalaTest {

  @Test
  def test(): Unit = {
    val ss = classOf[Int]
    val sss = classOf[Double]
    if (sss.isAssignableFrom(ss)) {
      Console.println(sss)
    }
  }

}
