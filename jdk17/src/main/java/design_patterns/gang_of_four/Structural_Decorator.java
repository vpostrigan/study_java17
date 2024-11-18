package design_patterns.gang_of_four;

// https://en.wikipedia.org/wiki/Decorator_pattern

/**
 * Декоратор (англ. Decorator) — структурный шаблон проектирования,
 * предназначенный для динамического подключения дополнительного поведения к объекту.
 * <p>
 * Шаблон Декоратор предоставляет гибкую альтернативу практике создания подклассов
 * с целью расширения функциональности.
 * (возможность динамически подключать новую функциональность до или после основной функциональности объекта)
 */
public class Structural_Decorator {

    public interface InterfaceComponent {
        void doOperation();
    }

    static class MainComponent implements InterfaceComponent {
        @Override
        public void doOperation() {
            System.out.print("World!");
        }
    }

    static abstract class Decorator implements InterfaceComponent {
        protected InterfaceComponent component;

        public Decorator(InterfaceComponent c) {
            component = c;
        }

        @Override
        public void doOperation() {
            component.doOperation();
        }

        public void newOperation() {
            System.out.println("Do Nothing");
        }
    }

    static class DecoratorSpace extends Decorator {

        public DecoratorSpace(InterfaceComponent c) {
            super(c);
        }

        @Override
        public void doOperation() {
            System.out.print(" ");
            super.doOperation();
        }

        @Override
        public void newOperation() {
            System.out.println("New space operation");
        }
    }

    static class DecoratorComma extends Decorator {

        public DecoratorComma(InterfaceComponent c) {
            super(c);
        }

        @Override
        public void doOperation() {
            System.out.print(",");
            super.doOperation();
        }

        @Override
        public void newOperation() {
            System.out.println("New comma operation");
        }
    }

    static class DecoratorHello extends Decorator {

        public DecoratorHello(InterfaceComponent c) {
            super(c);
        }

        @Override
        public void doOperation() {
            System.out.print("Hello");
            super.doOperation();
        }

        @Override
        public void newOperation() {
            System.out.println("New hello operation");
        }
    }


    public static void main(String... s) {
        Decorator c = new DecoratorHello(new DecoratorComma(new DecoratorSpace(new MainComponent())));
        c.doOperation(); // Результат выполнения программы "Hello, World!"
        c.newOperation(); // New hello operation
    }

}
