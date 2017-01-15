package com.qunar.test

import org.junit.Test

class ScalaTest {

  @Test
  def test(): Unit = {
    val ss = classOf[Seq[CharSequence]]
    val sss = classOf[Seq[Int]]
    if (sss.isAssignableFrom(ss)) {
      Console.println(sss)
    }
  }

}
