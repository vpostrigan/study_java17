package design_patterns.gang_of_four;

/**
 * Factory сосредотачивается на том, что именно создается, а Builder - на том, как оно создается
 */
public class Creational_Builder2 {

    private static class Computer {
        private String display = null;
        private String systemBlock = null;
        private String manipulators = null;

        public void setDisplay(String display) {
            this.display = display;
        }

        public void setSystemBlock(String systemBlock) {
            this.systemBlock = systemBlock;
        }

        public void setManipulators(String manipulators) {
            this.manipulators = manipulators;
        }
    }

    private static abstract class ComputerBuilder {
        protected Computer computer;

        public Computer getComputer() {
            return computer;
        }

        public void createNewComputer() {
            computer = new Computer();
        }

        public abstract void buildSystemBlock();

        public abstract void buildDisplay();

        public abstract void buildManipulators();
    }

    private static class CheapComputerBuilder extends ComputerBuilder {
        public void buildSystemBlock() {
            computer.setSystemBlock("Everest");
        }

        public void buildDisplay() {
            computer.setDisplay("CRT");
        }

        public void buildManipulators() {
            computer.setManipulators("mouse+keyboard");
        }
    }

    /**
     * использование паттерна Strategy (Множество построителей компьютера - ComputerBuilder)
     */
    private static class Director {
        private ComputerBuilder computerBuilder;

        public void setComputerBuilder(ComputerBuilder computerBuilder) {
            this.computerBuilder = computerBuilder;
        }

        public Computer getComputer() {
            return computerBuilder.getComputer();
        }

        public void constructComputer() {
            computerBuilder.createNewComputer();
            computerBuilder.buildSystemBlock();
            computerBuilder.buildDisplay();
            computerBuilder.buildManipulators();
        }
    }

    public static void main(String[] args) {
        Director director = new Director();
        ComputerBuilder cheapComputerBuilder = new CheapComputerBuilder();

        director.setComputerBuilder(cheapComputerBuilder);
        director.constructComputer();

        Computer computer = director.getComputer();
    }

}
