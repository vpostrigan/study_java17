package spark_in_action2021.part3transform_data

import org.apache.spark.sql.api.java.UDF2

@SerialVersionUID(-2162134L)
class Lab14_41IntegerAdditionScalaUdf extends UDF2[Int, Int, Int] {

  @throws[Exception]
  override def call(t1: Int, t2: Int): Int =
    t1 + t2

}

