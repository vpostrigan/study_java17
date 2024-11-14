package spark_in_action2021.part3transform_data;

import java.sql.Timestamp;

import org.apache.spark.sql.api.java.UDF8;

/**
 * The UDF code itself provides the plumbing between
 * the service code and the application code.
 *
 * @author jgp
 */
public class Lab14_12IsOpenUdf implements UDF8<String, String, String, String, String, String, String, Timestamp, Boolean> {
    private static final long serialVersionUID = -216751L;

    @Override
    public Boolean call(String hoursMon, String hoursTue, String hoursWed,
                        String hoursThu, String hoursFri, String hoursSat,
                        String hoursSun,
                        Timestamp dateTime) throws Exception {
        return Lab14_13IsOpenService.isOpen(hoursMon, hoursTue, hoursWed, hoursThu, hoursFri, hoursSat, hoursSun, dateTime);
    }

}
