package ch23.buffer;

import java.security.SecureRandom;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

// Fig. 23.13: SharedBufferTest.java
// Application with two threads manipulating an unsynchronized buffer.
public class SharedBufferTest {

    public static void main(String[] args) throws InterruptedException {
        // create new thread pool
        ExecutorService executorService = Executors.newCachedThreadPool();

        // create UnsynchronizedBuffer to store ints
        Buffer sharedLocation = new UnsynchronizedBuffer();

        System.out.println(
                "Action\t\tValue\tSum of Produced\tSum of Consumed");
        System.out.printf(
                "------\t\t-----\t---------------\t---------------%n%n");

        // execute the Producer and Consumer, giving each
        // access to the sharedLocation
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

    // Fig. 23.12: UnsynchronizedBuffer.java
    // UnsynchronizedBuffer maintains the shared integer that is accessed by
    // a producer thread and a consumer thread.
    static class UnsynchronizedBuffer implements Buffer {
        private int buffer = -1; // shared by producer and consumer threads

        // place value into buffer
        @Override
        public void blockingPut(int value) throws InterruptedException {
            System.out.printf("Producer writes\t%2d", value);
            buffer = value;
        }

        // return value from buffer
        @Override
        public int blockingGet() throws InterruptedException {
            System.out.printf("Consumer reads\t%2d", buffer);
            return buffer;
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
Action		Value	Sum of Produced	Sum of Consumed
------		-----	---------------	---------------

Producer writes	 1	 1
Producer writes	 2	 3
Producer writes	 3	 6
Consumer reads	 3			 3
Producer writes	 4	10
Consumer reads	 4			 7
Producer writes	 5	15
Producer writes	 6	21
Consumer reads	 6			13
Producer writes	 7	28
Consumer reads	 7			20
Producer writes	 8	36
Producer writes	 9	45
Consumer reads	 9			29
Consumer reads	 9			38
Consumer reads	 9			47
Producer writes	10	55
Producer done producing
Terminating Producer
Consumer reads	10			57
Consumer reads	10			67
Consumer reads	10			77

Consumer read values totaling 77
Terminating Consumer
 */
