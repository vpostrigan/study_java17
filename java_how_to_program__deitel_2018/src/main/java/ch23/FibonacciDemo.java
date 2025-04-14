package ch23;

import java.time.Duration;
import java.text.NumberFormat;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

// Fig. 23.31: FibonacciDemo.java
// Fibonacci calculations performed synchronously and asynchronously
public class FibonacciDemo {

    // class that stores two Instants in time
    static class TimeData {
        public Instant start;
        public Instant end;

        // return total time in seconds
        public double timeInSeconds() {
            return Duration.between(start, end).toMillis() / 1000.0;
        }
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        // perform synchronous fibonacci(45) and fibonacci(44) calculations
        System.out.println("Synchronous Long Running Calculations");
        TimeData synchronousResult1 = startFibonacci(45);
        TimeData synchronousResult2 = startFibonacci(44);
        double synchronousTime = calculateTime(synchronousResult1, synchronousResult2);
        System.out.printf("  Total calculation time = %.3f seconds%n", synchronousTime);

        // perform asynchronous fibonacci(45) and fibonacci(44) calculations
        System.out.printf("%nAsynchronous Long Running Calculations%n");
        CompletableFuture<TimeData> futureResult1 =
                CompletableFuture.supplyAsync(() -> startFibonacci(45));
        CompletableFuture<TimeData> futureResult2 =
                CompletableFuture.supplyAsync(() -> startFibonacci(44));

        // wait for results from the asynchronous operations
        TimeData asynchronousResult1 = futureResult1.get();
        TimeData asynchronousResult2 = futureResult2.get();
        double asynchronousTime = calculateTime(asynchronousResult1, asynchronousResult2);
        System.out.printf("  Total calculation time = %.3f seconds%n", asynchronousTime);

        // display time difference as a percentage
        String percentage = NumberFormat.getPercentInstance().format(
                (synchronousTime - asynchronousTime) / asynchronousTime);
        System.out.printf("%nSynchronous calculations took %s" +
                " more time than the asynchronous ones%n", percentage);
    }

    // executes function fibonacci asynchronously
    private static TimeData startFibonacci(int n) {
        // create a TimeData object to store times
        TimeData timeData = new TimeData();

        System.out.printf("  Calculating fibonacci(%d)%n", n);
        timeData.start = Instant.now();
        long fibonacciValue = fibonacci(n);
        timeData.end = Instant.now();
        displayResult(n, fibonacciValue, timeData);
        return timeData;
    }

    // recursive method fibonacci; calculates nth Fibonacci number
    private static long fibonacci(long n) {
        if (n == 0 || n == 1) {
            return n;
        } else {
            return fibonacci(n - 1) + fibonacci(n - 2);
        }
    }

    // display fibonacci calculation result and total calculation time
    private static void displayResult(int n, long value, TimeData timeData) {
        System.out.printf("  fibonacci(%d) = %d%n", n, value);
        System.out.printf("  Calculation time for fibonacci(%d) = %.3f seconds%n", n, timeData.timeInSeconds());
    }

    // display fibonacci calculation result and total calculation time
    private static double calculateTime(TimeData result1, TimeData result2) {
        TimeData bothThreads = new TimeData();
        // determine earlier start time
        bothThreads.start = result1.start.compareTo(result2.start) < 0 ? result1.start : result2.start;
        // determine later end time
        bothThreads.end = result1.end.compareTo(result2.end) > 0 ? result1.end : result2.end;

        return bothThreads.timeInSeconds();
    }

}
/*
Synchronous Long Running Calculations
  Calculating fibonacci(45)
  fibonacci(45) = 1134903170
  Calculation time for fibonacci(45) = 6.144 seconds
  Calculating fibonacci(44)
  fibonacci(44) = 701408733
  Calculation time for fibonacci(44) = 3.755 seconds
  Total calculation time = 9.907 seconds

Asynchronous Long Running Calculations
  Calculating fibonacci(44)
  Calculating fibonacci(45)
  fibonacci(44) = 701408733
  Calculation time for fibonacci(44) = 3.803 seconds
  fibonacci(45) = 1134903170
  Calculation time for fibonacci(45) = 6.227 seconds
  Total calculation time = 6.227 seconds

Synchronous calculations took 59% more time than the asynchronous ones
 */