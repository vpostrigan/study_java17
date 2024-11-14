package spark_in_action2021.part3transform_data

import org.apache.spark.sql.api.java.UDF2

/**
 * The UDF code itself provides the plumbing between the service code and
 * the application code.
 *
 * @author rambabu.posa
 *
 */
@SerialVersionUID(-21621751L)
class Lab14_32InRangeScalaUdf extends UDF2[String, Integer, Boolean] {

  def call(range: String, event: Integer): Boolean =
    Lab14_33InRangeScalaService.call(range, event)
}
