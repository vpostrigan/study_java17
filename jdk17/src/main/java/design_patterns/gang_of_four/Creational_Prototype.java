package design_patterns.gang_of_four;

/**
 * Example java.lang.Cloneable
 *
 * •	если создание объектов (через оператор new) занимает длительный промежуток времени или требовательно к памяти;
 * •	если создание объектов для клиента является нетривиальной задачей, например, когда объект составной;
 * •	избежать множества фабрик для создания конкретных экземпляров классов;
 * •	если клиент не знает специфики создания объекта.
 */
public class Creational_Prototype {

    interface Copyable {
        Copyable copy();
    }

    static class ComplicatedObject implements Copyable {
        public enum Type {
            ONE, TWO
        }

        @Override
        public ComplicatedObject copy() {
            return new ComplicatedObject();
        }

        Type type;

        public void setType(Type type) {
            this.type = type;
        }
    }

    public static void main(String[] args) {
        ComplicatedObject prototype = new ComplicatedObject();

        ComplicatedObject clone = prototype.copy();
        clone.setType(ComplicatedObject.Type.ONE);
    }

}
