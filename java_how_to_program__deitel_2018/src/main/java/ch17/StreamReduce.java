package ch17;

import java.util.stream.IntStream;

// Fig. 17.3: StreamReduce.java
// Sum the integers from 1 through 10 with IntStream.
public class StreamReduce {

    public static void main(String[] args) {
        // sum the integers from 1 through 10
        System.out.printf("Sum of 1 through 10 is: %d%n",
                IntStream.rangeClosed(1, 10)
                        .sum());
    }
/*
Sum of 1 through 10 is: 55
 */
}
