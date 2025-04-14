package ch23.buffer;

import java.security.SecureRandom;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

// Fig. 23.17: SharedBufferTest2.java
// Two threads correctly manipulating a synchronized buffer.
public class SharedBufferTest2 {

    public static void main(String[] args) throws InterruptedException {
        // create a newCachedThreadPool
        ExecutorService executorService = Executors.newCachedThreadPool();

        // create SynchronizedBuffer to store ints
        Buffer sharedLocation = new SynchronizedBuffer();

        System.out.printf("%-40s%s\t\t%s%n%-40s%s%n%n", "Operation",
                "Buffer", "Occupied", "---------", "------\t\t--------");

        // execute the Producer and Consumer tasks
        executorService.execute(new Producer(sharedLocation));
        executorService.execute(new Consumer(sharedLocation));

        executorService.shutdown(); // terminate app when tasks complete
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
                try { // sleep 0 to 3 seconds, then place value in Buffer
                    Thread.sleep(generator.nextInt(3000)); // random sleep
                    sharedLocation.blockingPut(count); // set value in buffer
                    sum += count; // increment sum of values
                    System.out.printf("\t%2d%n", sum);
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                }
            }

            System.out.printf("Producer done producing%nTerminating Producer%n");
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
                    System.out.printf("\t\t\t%2d%n", sum);
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                }
            }

            System.out.printf("%n%s %d%n%s%n",
                    "Consumer read values totaling", sum, "Terminating Consumer");
        }
    }

    // Fig. 23.16: SynchronizedBuffer.java
    // Synchronizing access to shared mutable data using Object methods wait and notifyAll.
    static class SynchronizedBuffer implements Buffer {
        private int buffer = -1; // shared by producer and consumer threads
        private boolean occupied = false;

        // place value into buffer
        @Override
        public synchronized void blockingPut(int value)
                throws InterruptedException {
            // while there are no empty locations, place thread in waiting state
            while (occupied) {
                // output thread information and buffer information, then wait
                System.out.println("Producer tries to write."); // for demo only
                displayState("Buffer full. Producer waits."); // for demo only
                wait();
            }

            buffer = value; // set new buffer value

            // indicate producer cannot store another value
            // until consumer retrieves current buffer value
            occupied = true;

            displayState("Producer writes " + buffer); // for demo only

            notifyAll(); // tell waiting thread(s) to enter runnable state
        } // end method blockingPut; releases lock on SynchronizedBuffer

        // return value from buffer
        @Override
        public synchronized int blockingGet() throws InterruptedException {
            // while no data to read, place thread in waiting state
            while (!occupied) {
                // output thread information and buffer information, then wait
                System.out.println("Consumer tries to read."); // for demo only
                displayState("Buffer empty. Consumer waits."); // for demo only
                wait();
            }

            // indicate that producer can store another value
            // because consumer just retrieved buffer value
            occupied = false;

            displayState("Consumer reads " + buffer); // for demo only

            notifyAll(); // tell waiting thread(s) to enter runnable state

            return buffer;
        } // end method blockingGet; releases lock on SynchronizedBuffer

        // display current operation and buffer state; for demo only
        private synchronized void displayState(String operation) {
            System.out.printf("%-40s%d\t\t%b%n%n", operation, buffer, occupied);
        }
    }

    // Fig. 23.9: Buffer.java
    // Buffer interface specifies methods called by Producer and Consumer.
    public interface Buffer {
        // place int value into Buffer
        void blockingPut(int value) throws InterruptedException;

        // return int value from Buffer
        int blockingGet() throws InterruptedException;
    }

}
/*
Operation                               Buffer		Occupied
---------                               ------		--------

Consumer tries to read.
Buffer empty. Consumer waits.           -1		false

Producer writes 1                       1		true

	 1
Consumer reads 1                        1		false

			 1
Consumer tries to read.
Buffer empty. Consumer waits.           1		false

Producer writes 2                       2		true

Consumer reads 2                        2		false

			 3
	 3
Producer writes 3                       3		true

	 6
Consumer reads 3                        3		false

			 6
Producer writes 4                       4		true

	10
Consumer reads 4                        4		false

			10
Consumer tries to read.
Buffer empty. Consumer waits.           4		false

Producer writes 5                       5		true

	15
Consumer reads 5                        5		false

			15
Consumer tries to read.
Buffer empty. Consumer waits.           5		false

Producer writes 6                       6		true

	21
Consumer reads 6                        6		false

			21
Producer writes 7                       7		true

	28
Consumer reads 7                        7		false

			28
Producer writes 8                       8		true

	36
Consumer reads 8                        8		false

			36
Producer writes 9                       9		true

	45
Consumer reads 9                        9		false

			45
Producer writes 10                      10		true

	55
Producer done producing
Terminating Producer
Consumer reads 10                       10		false

			55

Consumer read values totaling 55
Terminating Consumer
 */
