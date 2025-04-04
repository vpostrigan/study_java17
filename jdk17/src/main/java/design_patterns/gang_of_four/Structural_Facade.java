package design_patterns.gang_of_four;

public class Structural_Facade {


    // We’ll hide all the complexity in two methods: startEngine() and stopEngine().

    public class CarEngineFacade {
        private static int DEFAULT_COOLING_TEMP = 90;
        private static int MAX_ALLOWED_TEMP = 50;
        private Object fuelInjector = new Object();
        private Object airFlowController = new Object();
        private Object starter = new Object();
        private Object coolingController = new Object();
        private Object catalyticConverter = new Object();

        public void startEngine() {
            // fuelInjector.on();
            // airFlowController.takeAir();
            // fuelInjector.on();
            // fuelInjector.inject();
            // starter.start();
            // coolingController.setTemperatureUpperLimit(DEFAULT_COOLING_TEMP);
            // coolingController.run();
            // catalyticConverter.on();
        }

        public void stopEngine() {
            // fuelInjector.off();
            // catalyticConverter.off();
            // coolingController.cool(MAX_ALLOWED_TEMP);
            // coolingController.stop();
            // airFlowController.off();
        }
    }

    public static void main(String[] args) {
        // facade.startEngine();
        // ...
        // facade.stopEngine();
    }

}
