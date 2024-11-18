package javase;

import java.util.function.Supplier;

public class LoggerOptimization {
    private static final Logger log = new Logger();

    public static void main(String[] args) {
        // old
        if (log.isDebugEnabled()) {
            log.debug("text" + "text2");
        }

        // new
        log.debug(() -> "text" + "text2");
    }

    private static class Logger {
        boolean isDebugEnabled() {
            return false;
        }

        public void debug(String text) {
            // log text
        }

        // //

        public void debug(Supplier<String> sup) {
            if (isDebugEnabled())
                debug(sup.get());
        }
    }

}
