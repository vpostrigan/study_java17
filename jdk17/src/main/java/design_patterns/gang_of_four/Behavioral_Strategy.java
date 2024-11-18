package design_patterns.gang_of_four;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * https://www.youtube.com/watch?v=9GWS5dyZfJw
 */
public class Behavioral_Strategy {

    private static class CalculatorBefore {

        public int calculate(int a, int b, String operator) {
            if ("add".equals(operator)) {
                return a + b;
            } else if ("multiply".equals(operator)) {
                return a * b;
            } else if ("divide".equals(operator)) {
                return a / b;
            } else if ("subtract".equals(operator)) {
                return a - b;
            } else if ("mod".equals(operator)) {
                return a % b;
            } else {
                throw new IllegalStateException("Operation isn't supported.");
            }
        }
    }

    private static class CalculatorAfter {
        private Map<String, Operation> operations;

        public CalculatorAfter() {
            this.operations = new HashMap<>();
            this.operations.put("add", new Add());
            this.operations.put("multiply", new Multiply());
            this.operations.put("divide", new Divide());
            this.operations.put("subtract", new Subtract());
            this.operations.put("mod", new Mod());
        }

        public int calculate(int a, int b, String operator) {
            Operation operation = Optional.ofNullable(operations.get(operator))
                    .orElseThrow(() -> new IllegalStateException("No such operation."));
            return operation.execute(a, b);
        }

        public interface Operation {
            int execute(int a, int b);
        }

        private static class Add implements Operation {
            @Override
            public int execute(int a, int b) {
                return a + b;
            }
        }

        private static class Divide implements Operation {
            @Override
            public int execute(int a, int b) {
                return a / b;
            }
        }

        private static class Multiply implements Operation {
            @Override
            public int execute(int a, int b) {
                return a * b;
            }
        }

        private static class Subtract implements Operation {
            @Override
            public int execute(int a, int b) {
                return a - b;
            }
        }

        private static class Mod implements Operation {
            @Override
            public int execute(int a, int b) {
                return a % b;
            }
        }
    }

}
