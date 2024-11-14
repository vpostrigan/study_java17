package spark_in_action2021.part3transform_data

import org.apache.spark.sql.api.java.UDF2

@SerialVersionUID(-2162134L)
class Lab14_51StringAdditionScalaUdf extends UDF2[String, String, String] {

  @throws[Exception]
  override def call(t1: String, t2: String): String =
    t1 + t2

}
