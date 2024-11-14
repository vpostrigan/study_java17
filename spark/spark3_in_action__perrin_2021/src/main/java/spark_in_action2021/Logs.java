package spark_in_action2021;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicInteger;

public class Logs {
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private final AtomicInteger counter = new AtomicInteger();
    private final String folder;

    private final StringBuilder sb = new StringBuilder();

    public Logs() {
        String f = Thread.currentThread().getStackTrace()[2].getFileName();
        f = f.substring(0, f.lastIndexOf('.'));

        this.folder = System.getProperty("java.io.tmpdir") +
                "spark_in_action2021" + "/" + f + "/";
    }

    public void outPrintln(String s) {
        System.out.println(s);
        if (sb.length() > 0) {
            sb.append('\n');
        }
        sb.append(s);
    }

    public StringBuilder getSb() {
        return sb;
    }

    // //

    public String getFolder() {
        return folder;
    }

    public void show(String message, Dataset<?> df, int number, boolean truncate, boolean printSchema) {
        outPrintln(time() + message);

        df.show(number, truncate);
        if (printSchema) {
            df.printSchema();
        }

        outPrintln(time() + message + " has " + df.count() + ".");
        outPrintln("// //");
    }

    public void showAndSaveToCsv(String message, Dataset<?> df, boolean printSchema) {
        showAndSaveToCsv(message, df, 5, false, printSchema);
    }

    public void showAndSaveToCsv(String message, Dataset<?> df, int number, boolean truncate, boolean printSchema) {
        outPrintln(time() + message);

        df.show(number, truncate);
        if (printSchema) {
            df.printSchema();
        }

        String file = folder + counter.incrementAndGet() + "_csv";
        df = df.coalesce(1);
        try {
            df.write().format("csv")
                    .option("header", true)
                    .mode(SaveMode.Overwrite)
                    .save(file);
        } catch (Exception e) {
            e.printStackTrace();
        }

        outPrintln(time() + message + " has " + df.count() + ". Saved to " + file);
        outPrintln("// //");
    }

    private static String time() {
        return LocalTime.now().format(FORMATTER) + ": ";
    }

}
