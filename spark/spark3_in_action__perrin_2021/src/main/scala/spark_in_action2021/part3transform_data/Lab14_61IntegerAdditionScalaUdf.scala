package spark_in_action2021.part3transform_data

import org.apache.spark.sql.api.java.UDF2

/**
 * Return type to String
 *
 * @author rambabu.posa
 */
@SerialVersionUID(-2162134L)
class Lab14_61IntegerAdditionScalaUdf extends UDF2[Int, Int, String] {

  @throws[Exception]
  override def call(t1: Int, t2: Int): String =
    (t1 + t2).toString

}
