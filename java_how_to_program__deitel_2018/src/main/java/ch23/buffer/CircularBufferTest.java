package ch23.buffer;

import java.security.SecureRandom;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

// Fig. 23.19: CircularBufferTest.java
// Producer and Consumer threads correctly manipulating a circular buffer.
public class CircularBufferTest {

    public static void main(String[] args) throws InterruptedException {
        // create new thread pool
        ExecutorService executorService = Executors.newCachedThreadPool();

        // create CircularBuffer to store ints
        CircularBuffer sharedLocation = new CircularBuffer();

        // display the initial state of the CircularBuffer
        sharedLocation.displayState("Initial State");

        // execute the Producer and Consumer tasks
        executorService.execute(new Producer(sharedLocation));
        executorService.execute(new Consumer(sharedLocation));

        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);
    }

    // Fig. 23.10: Producer.java
    // Producer with a run method that inserts the values 1 to 10 in buffer.
    static class Producer implements Runnable {
        private static final SecureRandom generator = new SecureRandom();
        private final Buffer sharedLocation; // reference to shared object

        // constructor
        public Producer(Buffer sharedLocation) {
            this.sharedLocation = sharedLocation;
        }

        // store values from 1 to 10 in sharedLocation
        @Override
        public void run() {
            int sum = 0;

            for (int count = 1; count <= 10; count++) {
                // sleep 0 to 3 seconds, then place value in Buffer
                try {
                    Thread.sleep(generator.nextInt(3000)); // random sleep
                    sharedLocation.blockingPut(count); // set value in buffer
                    sum += count; // increment sum of values
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                }
            }

            System.out.printf("Producer done producing - Terminating Producer%n");
        }
    }

    // Fig. 23.11: Consumer.java
    // Consumer with a run method that loops, reading 10 values from buffer.
    static class Consumer implements Runnable {
        private static final SecureRandom generator = new SecureRandom();
        private final Buffer sharedLocation; // reference to shared object

        // constructor
        public Consumer(Buffer sharedLocation) {
            this.sharedLocation = sharedLocation;
        }

        // read sharedLocation's value 10 times and sum the values
        @Override
        public void run() {
            int sum = 0;

            for (int count = 1; count <= 10; count++) {
                // sleep 0 to 3 seconds, read value from buffer and add to sum
                try {
                    Thread.sleep(generator.nextInt(3000));
                    sum += sharedLocation.blockingGet();
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                }
            }

            System.out.printf("%n%s %d - %s%n",
                    "Consumer read values totaling", sum, "Terminating Consumer");
        }
    }

    // Fig. 23.18: CircularBuffer.java
    // Synchronizing access to a shared three-element bounded buffer.
    static class CircularBuffer implements Buffer {
        private final int[] buffer = {-1, -1, -1}; // shared buffer

        private int occupiedCells = 0; // count number of buffers used
        private int writeIndex = 0; // index of next element to write to
        private int readIndex = 0; // index of next element to read

        // place value into buffer
        @Override
        public synchronized void blockingPut(int value) throws InterruptedException {

            // wait until buffer has space available, then write value;
            // while no empty locations, place thread in blocked state
            while (occupiedCells == buffer.length) {
                System.out.printf("Buffer is full. Producer waits.%n");
                wait(); // wait until a buffer cell is free
            }

            buffer[writeIndex] = value; // set new buffer value

            // update circular write index
            writeIndex = (writeIndex + 1) % buffer.length;

            ++occupiedCells; // one more buffer cell is full
            displayState("Producer writes " + value);
            notifyAll(); // notify threads waiting to read from buffer
        }

        // return value from buffer
        @Override
        public synchronized int blockingGet() throws InterruptedException {
            // wait until buffer has data, then read value;
            // while no data to read, place thread in waiting state
            while (occupiedCells == 0) {
                System.out.printf("Buffer is empty. Consumer waits.%n");
                wait(); // wait until a buffer cell is filled
            }

            int readValue = buffer[readIndex]; // read value from buffer

            // update circular read index
            readIndex = (readIndex + 1) % buffer.length;

            --occupiedCells; // one fewer buffer cells are occupied
            displayState("Consumer reads " + readValue);
            notifyAll(); // notify threads waiting to write to buffer

            return readValue;
        }

        // display current operation and buffer state
        public synchronized void displayState(String operation) {
            // output operation and number of occupied buffer cells
            System.out.printf("%s%s%d)%n%s", operation,
                    " (buffer cells occupied: ", occupiedCells, "buffer cells:  ");

            for (int value : buffer) {
                System.out.printf(" %2d  ", value); // output values in buffer
            }
            System.out.printf("%n               ");

            for (int i = 0; i < buffer.length; i++) {
                System.out.print("---- ");
            }
            System.out.printf("%n               ");

            for (int i = 0; i < buffer.length; i++) {
                if (i == writeIndex && i == readIndex) {
                    System.out.print(" WR"); // both write and read index
                } else if (i == writeIndex) {
                    System.out.print(" W   "); // just write index
                } else if (i == readIndex) {
                    System.out.print("  R  "); // just read index
                } else {
                    System.out.print("     "); // neither index
                }
            }
            System.out.printf("%n%n");
        }
    }

    // Fig. 23.9: Buffer.java
    // Buffer interface specifies methods called by Producer and Consumer.
    public interface Buffer {
        // place int value into Buffer
        public void blockingPut(int value) throws InterruptedException;

        // return int value from Buffer
        public int blockingGet() throws InterruptedException;
    }
/*
Initial State (buffer cells occupied: 0)
buffer cells:   -1   -1   -1
               ---- ---- ----
                WR

Producer writes 1 (buffer cells occupied: 1)
buffer cells:    1   -1   -1
               ---- ---- ----
                 R   W

Producer writes 2 (buffer cells occupied: 2)
buffer cells:    1    2   -1
               ---- ---- ----
                 R        W

Producer writes 3 (buffer cells occupied: 3)
buffer cells:    1    2    3
               ---- ---- ----
                WR

Consumer reads 1 (buffer cells occupied: 2)
buffer cells:    1    2    3
               ---- ---- ----
                W     R

Consumer reads 2 (buffer cells occupied: 1)
buffer cells:    1    2    3
               ---- ---- ----
                W          R

Consumer reads 3 (buffer cells occupied: 0)
buffer cells:    1    2    3
               ---- ---- ----
                WR

Producer writes 4 (buffer cells occupied: 1)
buffer cells:    4    2    3
               ---- ---- ----
                 R   W

Producer writes 5 (buffer cells occupied: 2)
buffer cells:    4    5    3
               ---- ---- ----
                 R        W

Consumer reads 4 (buffer cells occupied: 1)
buffer cells:    4    5    3
               ---- ---- ----
                      R   W

Producer writes 6 (buffer cells occupied: 2)
buffer cells:    4    5    6
               ---- ---- ----
                W     R

Producer writes 7 (buffer cells occupied: 3)
buffer cells:    7    5    6
               ---- ---- ----
                     WR

Consumer reads 5 (buffer cells occupied: 2)
buffer cells:    7    5    6
               ---- ---- ----
                     W     R

Producer writes 8 (buffer cells occupied: 3)
buffer cells:    7    8    6
               ---- ---- ----
                          WR

Buffer is full. Producer waits.
Consumer reads 6 (buffer cells occupied: 2)
buffer cells:    7    8    6
               ---- ---- ----
                 R        W

Producer writes 9 (buffer cells occupied: 3)
buffer cells:    7    8    9
               ---- ---- ----
                WR

Consumer reads 7 (buffer cells occupied: 2)
buffer cells:    7    8    9
               ---- ---- ----
                W     R

Consumer reads 8 (buffer cells occupied: 1)
buffer cells:    7    8    9
               ---- ---- ----
                W          R

Producer writes 10 (buffer cells occupied: 2)
buffer cells:   10    8    9
               ---- ---- ----
                     W     R

Producer done producing - Terminating Producer
Consumer reads 9 (buffer cells occupied: 1)
buffer cells:   10    8    9
               ---- ---- ----
                 R   W

Consumer reads 10 (buffer cells occupied: 0)
buffer cells:   10    8    9
               ---- ---- ----
                     WR


Consumer read values totaling 55 - Terminating Consumer
 */
}
