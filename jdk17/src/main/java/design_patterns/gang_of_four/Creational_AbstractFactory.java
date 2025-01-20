package design_patterns.gang_of_four;

/**
 * •	система не должна зависеть от способа создания объектов;
 * •	система работает с одним из нескольких семейств объектов;
 * •	объекты внутри семейства взаимосвязаны.
 */
public class Creational_AbstractFactory {

    public void createSquadron(SquadronFactory factory) {
        Mage mage = factory.createMage();
        Archer archer = factory.createArcher();
        Warrior warrior = factory.createWarrior();
        // ...
    }


    public abstract class SquadronFactory {
        public abstract Mage createMage();

        public abstract Archer createArcher();

        public abstract Warrior createWarrior();
    }

    // //

    public interface Mage {
        void cast();
    }

    public interface Archer {
        void shoot();
    }

    public interface Warrior {
        void attack();
    }

    // //

    public class ElfSquadronFactory extends SquadronFactory {
        public Mage createMage() {
            return new ElfMage();
        }

        public Archer createArcher() {
            return new ElfArcher();
        }

        public Warrior createWarrior() {
            return new ElfWarrior();
        }
    }

    public class ElfMage implements Mage {
        public void cast() {
            // использовать магию эльфов
        }
    }

    public class ElfArcher implements Archer {
        public void shoot() {
            // использовать лук эльфов
        }
    }

    public class ElfWarrior implements Warrior {
        public void attack() {
            // использовать меч эльфов
        }
    }

}
