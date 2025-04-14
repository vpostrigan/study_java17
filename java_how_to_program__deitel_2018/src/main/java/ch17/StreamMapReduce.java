package ch17;

import java.util.stream.IntStream;

// Fig. 17.4: StreamMapReduce.java
// Sum the even integers from 2 through 20 with IntStream.
public class StreamMapReduce {

    public static void main(String[] args) {
        // sum the even integers from 2 through 20
        System.out.printf("Sum of the even ints from 2 through 20 is: %d%n",
                IntStream.rangeClosed(1, 10)              // 1...10
                        .map((int x) -> {
                            return x * 2;
                        }) // multiply by 2
                        .sum());                         // sum
    }
/*
Sum of the even ints from 2 through 20 is: 110
 */
}
