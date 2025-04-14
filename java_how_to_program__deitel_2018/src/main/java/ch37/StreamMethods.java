package ch37;

import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

// Fig. 37.2: StreamMethods.java
// Java 9's new stream methods takeWhile, dropWhile, iterate and ofNullable.
public class StreamMethods {

    public static void main(String[] args) {
        int[] values = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

        System.out.printf("Array values contains: %s%n",
                IntStream.of(values)
                        .mapToObj(String::valueOf)
                        .collect(Collectors.joining(" ")));
        // Array values contains: 1 2 3 4 5 6 7 8 9 10

        // take the largest stream prefix of elements less than 6
        System.out.println("Demonstrating takeWhile and dropWhile:");
        System.out.printf("Elements less than 6: %s%n",
                IntStream.of(values)
                        .takeWhile(e -> e < 6)
                        .mapToObj(String::valueOf)
                        .collect(Collectors.joining(" ")));
        // Elements less than 6: 1 2 3 4 5

        // drop the largest stream prefix of elements less than 6
        System.out.printf("Elements 6 or greater: %s%n",
                IntStream.of(values)
                        .dropWhile(e -> e < 6)
                        .mapToObj(String::valueOf)
                        .collect(Collectors.joining(" ")));
        // Elements 6 or greater: 6 7 8 9 10

        // use iterate to generate stream of powers of 3 less than 10000
        System.out.printf("%nDemonstrating iterate:%n");
        System.out.printf("Powers of 3 less than 10,000: %s%n",
                IntStream.iterate(3, n -> n < 10_000, n -> n * 3)
                        .mapToObj(String::valueOf)
                        .collect(Collectors.joining(" ")));
        // Powers of 3 less than 10,000: 3 9 27 81 243 729 2187 6561

        // demonstrating ofNullable
        System.out.printf("%nDemonstrating ofNullable:%n");
        System.out.printf("Number of stream elements: %d%n", Stream.ofNullable(null).count());
        System.out.printf("Number of stream elements: %d%n", Stream.ofNullable("red").count());
        // Number of stream elements: 0
        // Number of stream elements: 1
    }

}
