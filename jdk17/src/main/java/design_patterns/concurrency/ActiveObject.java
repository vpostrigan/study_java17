package design_patterns.concurrency;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * https://en.wikipedia.org/wiki/Active_object
 */
public class ActiveObject {

    /**
     * The class is dangerous in a multithreading scenario because both methods
     * can be called simultaneously, so the value of val
     * (which is not atomic — it's updated in multiple steps) could be undefined —
     * a classic race condition.
     */
    class MyClassBefore {
        private double val = 0.0;

        void doSomething() {
            val = 1.0;
        }

        void doSomethingElse() {
            val = 2.0;
        }
    }

    class MyClassAfter {
        private double val = 0.0;
        private BlockingQueue<Runnable> dispatchQueue = new LinkedBlockingQueue<>();

        public MyClassAfter() {
            new Thread(() -> {
                try {
                    while (true) {
                        dispatchQueue.take().run();
                    }
                } catch (InterruptedException e) {
                    // okay, just terminate the dispatcher
                }
            }).start();
        }

        void doSomething() throws InterruptedException {
            dispatchQueue.put(() -> val = 1.0);
        }

        void doSomethingElse() throws InterruptedException {
            dispatchQueue.put(() -> val = 2.0);
        }
    }

    class MyClassAfterJava8 {
        private double val;

        // container for tasks decides which request to execute next
        // asyncMode=true means our worker thread processes its local task queue in the FIFO order
        // only single thread may modify internal state
        private final ForkJoinPool fj =
                new ForkJoinPool(1, ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, true);

        // implementation of active object method
        public void doSomething() {
            fj.execute(() -> {
                val = 1.0;
            });
        }

        // implementation of active object method
        public void doSomethingElse() {
            fj.execute(() -> {
                val = 2.0;
            });
        }
    }

}
