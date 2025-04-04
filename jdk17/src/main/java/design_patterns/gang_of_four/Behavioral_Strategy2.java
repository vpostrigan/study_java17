package design_patterns.gang_of_four;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * https://www.youtube.com/watch?v=9GWS5dyZfJw
 */
public class Behavioral_Strategy2 {

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

    // //

    private static class CalculatorAfter {
        private Map<String, CalculatorAfter.Operation> operations;

        public CalculatorAfter() {
            this.operations = new HashMap<>();
            this.operations.put("add", new CalculatorAfter.Add());
            this.operations.put("multiply", new CalculatorAfter.Multiply());
            this.operations.put("divide", new CalculatorAfter.Divide());
            this.operations.put("subtract", new CalculatorAfter.Subtract());
            this.operations.put("mod", new CalculatorAfter.Mod());
        }

        public int calculate(int a, int b, String operator) {
            CalculatorAfter.Operation operation =
                    Optional.ofNullable(operations.get(operator))
                            .orElseThrow(() -> new IllegalStateException("No such operation."));
            return operation.execute(a, b);
        }

        public interface Operation {
            int execute(int a, int b);
        }

        private static class Add implements CalculatorAfter.Operation {
            @Override
            public int execute(int a, int b) {
                return a + b;
            }
        }

        private static class Divide implements CalculatorAfter.Operation {
            @Override
            public int execute(int a, int b) {
                return a / b;
            }
        }

        private static class Multiply implements CalculatorAfter.Operation {
            @Override
            public int execute(int a, int b) {
                return a * b;
            }
        }

        private static class Subtract implements CalculatorAfter.Operation {
            @Override
            public int execute(int a, int b) {
                return a - b;
            }
        }

        private static class Mod implements CalculatorAfter.Operation {
            @Override
            public int execute(int a, int b) {
                return a % b;
            }
        }
    }

}
