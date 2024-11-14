package spark_in_action2021.part3transform_data;

import java.util.List;

import org.apache.spark.sql.api.java.UDF1;

import scala.collection.Seq;

public class Lab14_72ColumnAdditionUdf implements UDF1<Seq<Integer>, Integer> {
    private static final long serialVersionUID = 8331L;

    @Override
    public Integer call(Seq<Integer> t1) throws Exception {
        List<Integer> integers =
                scala.collection.JavaConverters.seqAsJavaListConverter(t1).asJava();
        int res = 0;
        for (int val : integers) {
            res += val;
        }
        return res;
    }

}
