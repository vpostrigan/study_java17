package spark_in_action2021.streaming.lib;

/**
 * Specific exception to the record generator.
 *
 * @author jgp
 */
public class RecordGeneratorException extends Exception {
    private static final long serialVersionUID = 4046912590125990484L;

    public RecordGeneratorException(String message, Exception e) {
        super(message, e);
    }

}
