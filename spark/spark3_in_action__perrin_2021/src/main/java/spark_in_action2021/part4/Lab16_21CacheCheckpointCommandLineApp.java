package spark_in_action2021.part4;

/**
 * Measuring performance without cache, with cache, and with checkpoint.
 * <p>
 * Can be run via the command line:
 *
 * @formatter:off
 *
 * mvn -q exec:exec -Dargs="100000" -DXmx=4G 2>/dev/null
 *                         |              |
 *                         |              +-- 4GB for Xmx Java memory,
 *                         |                  the more records you will
 *                         |                  process, the more you will need.
 *                         |
 *                         +-- Number of records to create.
 * @formatter:on
 */
public class Lab16_21CacheCheckpointCommandLineApp {

    public static void main(String[] args) {
        if (args.length == 0) {
            return;
        }

        int recordCount = Integer.parseInt(args[0]);
        String master;
        if (args.length > 1) {
            master = args[1];
        } else {
            master = "local[*]";
        }

        Lab16_11CacheCheckpointApp app = new Lab16_11CacheCheckpointApp();
        app.start(recordCount, master);
    }

}
