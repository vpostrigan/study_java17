package design_patterns.gang_of_four;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Publisher-Subscriber (Издатель-Подписчик)
 *
 * •	Если один объект должен передавать сообщения другим объектам,
 *           но при этом он не может или не должен знать об их внутреннем устройстве;
 * •	В случае если при изменении одного объекта необходимо изменять другие объекты;
 * •	Для предотвращения сильных связей между объектами системы;
 * •	Для наблюдения за состоянием определенных объектов системы;
 */
public class Behavioral_Observer {

    interface Observer {
        void objectCreated(Object obj);

        void objectModified(Object obj);
    }

    static class EmptyObserver implements Observer {
        public void objectCreated(Object obj) {
        }

        public void objectModified(Object obj) {
        }
    }

    static class Observers<T extends Observer> extends ArrayList<T> {
        public void notifyObjectCreated(Object obj) {
            for (Iterator<T> iter = (Iterator<T>) iterator(); iter.hasNext(); )
                iter.next().objectCreated(null);
        }

        public void notifyObjectModified(Object obj) {
            for (Iterator<T> iter = (Iterator<T>) iterator(); iter.hasNext(); )
                iter.next().objectCreated(null);
        }
    }

    public static void main(String[] args) {
        Observers observers = new Observers();

        observers.add(new EmptyObserver() {
            public void objectCreated(Object obj) { /* реализация */ }
        });

    }

    /**
     * экземпляр класса Observers помещается в субъект за которым должно вестись наблюдение
     */
    static class Subject {
        Observers observers = new Observers();

        private Object field;

        public void setField(Object o) {
            field = o;
            observers.notifyObjectModified(this);
        }
    }

}
