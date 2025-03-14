package design_patterns.gang_of_four;

/**
 * его еще называют Wrapper
 *
 * используется для того, чтобы объекты с разными интерфейсами могли работать друг с другом.
 * Существует два типа адаптеров - Class Adapter и Object Adapter
 *
 * Facade определяет новый интерфейс, в то время как Адаптер использует существующие интерфейсы
 */
public class Structural_Adapter {

    // ObjectAdapter
    interface TargetInterface {
        void targetMethod();
    }

    class Adaptee {
        void method() {
        }
    }

    class ObjectAdapter implements TargetInterface {
        private Adaptee adaptee;

        public void targetMethod() {
            adaptee.method();
        }
    }

    // Class Adapter (может возникнуть конфликт сигратур методов)
    public class ClassAdapter extends Adaptee implements TargetInterface {
        public void targetMethod() {
            method();
        }
    }


}
