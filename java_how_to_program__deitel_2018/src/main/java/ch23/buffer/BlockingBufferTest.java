package ch23.buffer;

import java.security.SecureRandom;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

// Fig. 23.15: BlockingBufferTest.java
// Two threads manipulating a blocking buffer that properly
// implements the producer/consumer relationship.
public class BlockingBufferTest {

    public static void main(String[] args) throws InterruptedException {
        // create new thread pool
        ExecutorService executorService = Executors.newCachedThreadPool();

        // create BlockingBuffer to store ints
        Buffer sharedLocation = new BlockingBuffer();

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

    // Fig. 23.14: BlockingBuffer.java
    // Creating a synchronized buffer using an ArrayBlockingQueue.
    static class BlockingBuffer implements Buffer {
        private final ArrayBlockingQueue<Integer> buffer; // shared buffer

        public BlockingBuffer() {
            buffer = new ArrayBlockingQueue<>(1);
        }

        // place value into buffer
        @Override
        public void blockingPut(int value) throws InterruptedException {
            buffer.put(value); // place value in buffer
            System.out.printf("%s%2d\t%s%d%n", "Producer writes ", value,
                    "Buffer cells occupied: ", buffer.size());
        }

        // return value from buffer
        @Override
        public int blockingGet() throws InterruptedException {
            int readValue = buffer.take(); // remove value from buffer
            System.out.printf("%s %2d\t%s%d%n", "Consumer reads ", readValue,
                    "Buffer cells occupied: ", buffer.size());

            return readValue;
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
/*
Producer writes  1	Buffer cells occupied: 1
Consumer reads   1	Buffer cells occupied: 0
Producer writes  2	Buffer cells occupied: 1
Consumer reads   2	Buffer cells occupied: 0
Producer writes  3	Buffer cells occupied: 1
Consumer reads   3	Buffer cells occupied: 0
Producer writes  4	Buffer cells occupied: 1
Consumer reads   4	Buffer cells occupied: 0
Producer writes  5	Buffer cells occupied: 1
Consumer reads   5	Buffer cells occupied: 0
Producer writes  6	Buffer cells occupied: 1
Consumer reads   6	Buffer cells occupied: 0
Producer writes  7	Buffer cells occupied: 1
Consumer reads   7	Buffer cells occupied: 0
Producer writes  8	Buffer cells occupied: 1
Consumer reads   8	Buffer cells occupied: 0
Producer writes  9	Buffer cells occupied: 1
Consumer reads   9	Buffer cells occupied: 0
Producer writes 10	Buffer cells occupied: 1
Producer done producing - Terminating Producer
Consumer reads  10	Buffer cells occupied: 0

Consumer read values totaling 55 - Terminating Consumer
*/
}
