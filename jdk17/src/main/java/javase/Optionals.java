package javase;

import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * https://www.youtube.com/watch?v=1ia8B3SUie8
 * JAVA 8 | OPTIONAL | КАК ИЗБАВИТЬСЯ ОТ IF ELSE
 * <p>
 * https://www.youtube.com/watch?v=XNEJo0IiJeg
 * КАК ЗАМЕНИТЬ IF ELSE И SWITCH CASE В JAVA 11 | РЕФАКТОРИНГ
 */
public class Optionals {

    public static void main(String[] args) {
        Optional o1 = Optional.ofNullable(null);
        // System.out.println(o1.get()); java.util.NoSuchElementException: No value present
        System.out.println(o1.orElse(null)); // null

        Optional o2 = Optional.ofNullable(new Object());
        System.out.println(o2.get()); // java.lang.Object@27f674d

        // //

        // Optional o21 = Optional.of(null); java.lang.NullPointerException
        Optional o22 = Optional.of(new Object());
        System.out.println(o2.get()); // java.lang.Object@27f674d

        // //

        // before Optional
        String r;
        if (args == null) {
            r = "arg is null";
        } else {
            r = "arg is not null";
        }

        // with Optional
        r = Optional.ofNullable(args)
                .map(v -> "arg is not null")
                .orElseGet(() -> "arg is null");
        System.out.println(r);

        // //

        // before Optional
        if (args == null) {
            r = "arg is null";
        } else {
            r = "arg is not null";
        }

        // with Optional
        Optional.ofNullable(args)
                .ifPresentOrElse(
                        v -> System.out.println("arg is not null"),
                        () -> System.out.println("arg is null"));

        // //

        // before Optional
        String str = "value";
        if (str != null && !str.isEmpty() && str.contains("value")) {
            System.out.println(str);
        } else {
            throw new RuntimeException();
        }

        // with Optional
        String result = Optional.ofNullable(str)
                .filter(s -> !s.isEmpty())
                .filter(s -> s.contains("value"))
                .orElseThrow(RuntimeException::new);
        System.out.println(result);

        result = Optional.ofNullable(str)
                .filter(s -> !s.isEmpty())
                .filter(s -> s.contains("value"))
                .orElse("default value"); // return default value
        System.out.println(result);

        Optional.ofNullable(str)
                .filter(s -> !s.isEmpty())
                .filter(s -> s.contains("value"))
                .ifPresent(System.out::println); // ifPresent - вызов метода

        // //

        // before Optional
        switchReplace1(str);

        // with Optional
        Map<String, Supplier<String>> strategyMap = Map.of(
                "arg1", () -> {
                    System.out.println("arg1 out");
                    return "arg1 out";
                },
                "arg2", () -> {
                    System.out.println("arg2 out");
                    return "arg2 out";
                },
                "arg3", () -> {
                    System.out.println("arg3 out");
                    return "arg3 out";
                }
        );
        Optional.ofNullable(strategyMap.get(str))
                .orElse(() -> {
                    System.out.println("default out");
                    return "default out";
                })
                .get();

        // before Optional
        switchReplace2(str);

        // with Optional
        Map<String, Consumer<String>> strategyMap2 = Map.of(
                "arg1", (v) -> System.out.println("arg1 out"),
                "arg2", (v) -> System.out.println("arg2 out"),
                "arg3", (v) -> System.out.println("arg3 out")
        );
        Optional.ofNullable(strategyMap2.get(str))
                .orElse((v) -> System.out.println("default out"))
                .accept(str);
    }

    private static String switchReplace1(String str) {
        switch (str) {
            case "arg1":
                System.out.println("arg1 out");
                return "arg1 out";
            case "arg2":
                System.out.println("arg2 out");
                return "arg2 out";
            case "arg3":
                System.out.println("arg3 out");
                return "arg3 out";
            default:
                System.out.println("default out");
                return "default out";
        }
    }

    private static void switchReplace2(String str) {
        switch (str) {
            case "arg1":
                System.out.println("arg1 out");
            case "arg2":
                System.out.println("arg2 out");
            case "arg3":
                System.out.println("arg3 out");
            default:
                System.out.println("default out");
        }
    }

}
