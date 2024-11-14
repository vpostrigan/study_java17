package spark_in_action2021.part3transform_data

import java.sql.Timestamp
import org.apache.spark.sql.api.java.UDF8

/**
 * The UDF code itself provides the plumbing between the service code and
 * the application code.
 *
 * @author rambabu.posa
 *
 */
@SerialVersionUID(-216751L)
class Lab14_12IsOpenScalaUdf extends
  UDF8[String, String, String, String, String, String, String, Timestamp, Boolean] {

  @throws[Exception]
  override def call(hoursMon: String, hoursTue: String,
                    hoursWed: String, hoursThu: String,
                    hoursFri: String, hoursSat: String,
                    hoursSun: String, dateTime: Timestamp): Boolean =
    Lab14_13IsOpenScalaService.isOpen(hoursMon, hoursTue, hoursWed,
      hoursThu, hoursFri, hoursSat, hoursSun, dateTime)

}
