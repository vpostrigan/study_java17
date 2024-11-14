package spark_in_action2021.part3transform_data;

import org.apache.spark.sql.api.java.UDF2;

/**
 * Return type to String
 *
 * @author jgp
 */
public class Lab14_61IntegerAdditionUdf implements UDF2<Integer, Integer, String> {

    private static final long serialVersionUID = -2162134L;

    @Override
    public String call(Integer t1, Integer t2) throws Exception {
        return Integer.toString(t1 + t2);
    }

}
