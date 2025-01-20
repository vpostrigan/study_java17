package design_patterns.gang_of_four;

import java.io.File;

import org.w3c.dom.Document;

/**
 * Паттерн Factory Method (Фабрика)
 *
 * Factory Method - это паттерн создания объектов (creational pattern).
 * Данный шаблон проектирования предоставляет интерфейс для создания экземпляров некоторого класса.
 * В момент создания наследники могут определить, какой класс инстанциировать.
 * Другими словами, Фабрика делегирует создание объектов наследникам родительского класса.
 * Это позволяет использовать в коде программы не специфические классы,
 * а манипулировать абстрактными объектами на более высоком уровне.
 *
 * Используйте паттерн Factory в следующих случаях:
 * •	класс не имеет информации о том, какой тип объекта он должен создать;
 * •	класс передает ответственность по созданию объектов наследникам;
 * •	необходимо создать объект в зависимости от входящих данных.
 */
public class Creational_FactoryMethod {

    /**
     * AbstractWriter будет представлять абстракцию для записи в некоторый контекст
     * (будь то XML-документ или текстовый файл)
     */
    public abstract class AbstractWriter {
        public abstract void write(Object context);
    }

    public class ConcreteFileWriter extends AbstractWriter {
        public void write(Object context) {
            // method body
        }
    }

    public class ConcreteXmlWriter extends AbstractWriter {
        public void write(Object context) {
            // method body
        }
    }

    // Для создания нужного нам объекта можно написать следующую Фабрику:
    public class FactoryMethod {

        public AbstractWriter getWriter(Object object) {
            AbstractWriter writer = null;
            if (object instanceof File) {
                writer = new ConcreteFileWriter();
            } else if (object instanceof Document) {
                writer = new ConcreteXmlWriter();
            }
            return writer;
        }
    }

}
