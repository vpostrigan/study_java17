package spark_in_action2021.streaming.lib;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes a record to an output.
 *
 * @author jgp
 */
public abstract class RecordWriterUtils {
    private static final Logger log = LoggerFactory.getLogger(RecordWriterUtils.class);

    public static String inputDirectory;
    public static String inputDirectory2;
    public static String inputSubDirectory1;
    public static String inputSubDirectory2;

    static {
        if (System.getProperty("os.name").toLowerCase().startsWith("win")) {
            inputDirectory = "C:\\TEMP\\";
        } else {
            inputDirectory = System.getProperty("java.io.tmpdir") + File.separator;
        }
        inputDirectory += "streaming" + File.separator + "in" + File.separator;

        if (System.getProperty("os.name").toLowerCase().startsWith("win")) {
            inputDirectory2 = "C:\\TEMP\\";
        } else {
            inputDirectory2 = System.getProperty("java.io.tmpdir") + File.separator;
        }
        inputDirectory2 += "streaming2" + File.separator + "in" + File.separator;

        File d = new File(inputDirectory);
        d.mkdirs();

        d = new File(inputSubDirectory1);
        d.mkdirs();

        d = new File(inputSubDirectory2);
        d.mkdirs();
    }

    public static void write(String filename, StringBuilder record) {
        write(filename, record, inputDirectory);
    }

    /**
     * Write a record to a file.
     */
    public static void write(String filename, StringBuilder record, String directory) {
        if (!directory.endsWith(File.separator)) {
            directory += File.separator;
        }
        String fullFilename = directory + filename;

        log.info("Writing in: {}", fullFilename);

        try (FileWriter f = new FileWriter(fullFilename, true); // true tells to append data.
             BufferedWriter out = new BufferedWriter(f);) {
            out.write(record.toString());
        } catch (IOException e) {
            log.error("Error while writing: {}", e.getMessage());
        }
    }

}
