package ch23;

import java.security.SecureRandom;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

// Fig. 23.7: SharedArrayTest.java
// Executing two Runnables to add elements to a shared SimpleArray.
public class SharedArrayTest2 {

    public static void main(String[] arg) {
        // construct the shared object
        SimpleArray sharedSimpleArray = new SimpleArray(6);

        // create two tasks to write to the shared SimpleArray
        ArrayWriter writer1 = new ArrayWriter(1, sharedSimpleArray);
        ArrayWriter writer2 = new ArrayWriter(11, sharedSimpleArray);

        // execute the tasks with an ExecutorService
        ExecutorService executorService = Executors.newCachedThreadPool();
        executorService.execute(writer1);
        executorService.execute(writer2);

        executorService.shutdown();

        try {
            // wait 1 minute for both writers to finish executing
            boolean tasksEnded = executorService.awaitTermination(1, TimeUnit.MINUTES);

            if (tasksEnded) {
                System.out.printf("%nContents of SimpleArray:%n");
                System.out.println(sharedSimpleArray); // print contents
            } else {
                System.out.println("Timed out while waiting for tasks to finish.");
            }
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
    }

    // Fig. 23.6: ArrayWriter.java
    // Adds integers to an array shared with other Runnables
    static class ArrayWriter implements Runnable {
        private final SimpleArray sharedSimpleArray;
        private final int startValue;

        public ArrayWriter(int value, SimpleArray array) {
            startValue = value;
            sharedSimpleArray = array;
        }

        @Override
        public void run() {
            for (int i = startValue; i < startValue + 3; i++) {
                sharedSimpleArray.add(i); // add an element to the shared array
            }
        }

    }

    // Fig. 23.8: SimpleArray.java
    // Class that manages an integer array to be shared by multiple threads with synchronization.
    static class SimpleArray {
        private static final SecureRandom generator = new SecureRandom();

        private final int[] array; // the shared integer array
        private int writeIndex = 0; // index of next element to be written

        // construct a SimpleArray of a given size
        public SimpleArray(int size) {
            array = new int[size];
        }

        // add a value to the shared array
        public synchronized void add(int value) {
            int position = writeIndex; // store the write index

            try {
                // in real applications, you shouldn't sleep while holding a lock
                Thread.sleep(generator.nextInt(500)); // for demo only
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }

            // put value in the appropriate element
            array[position] = value;
            System.out.printf("%s wrote %2d to element %d.%n",
                    Thread.currentThread().getName(), value, position);

            ++writeIndex; // increment index of element to be written next
            System.out.printf("Next write index: %d%n", writeIndex);
        }

        // used for outputting the contents of the shared integer array
        @Override
        public synchronized String toString() {
            return Arrays.toString(array);
        }

    }

}
/*
pool-1-thread-1 wrote  1 to element 0.
Next write index: 1
pool-1-thread-1 wrote  2 to element 1.
Next write index: 2
pool-1-thread-2 wrote 11 to element 2.
Next write index: 3
pool-1-thread-1 wrote  3 to element 3.
Next write index: 4
pool-1-thread-2 wrote 12 to element 4.
Next write index: 5
pool-1-thread-2 wrote 13 to element 5.
Next write index: 6

Contents of SimpleArray:
[1, 2, 11, 3, 12, 13]
 */
