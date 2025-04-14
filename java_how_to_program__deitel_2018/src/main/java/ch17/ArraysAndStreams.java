package ch17;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

// Fig. 17.11: ArraysAndStreams.java
// Demonstrating lambdas and streams with an array of Integers.
public class ArraysAndStreams {

    public static void main(String[] args) {
        Integer[] values = {2, 9, 5, 0, 3, 7, 1, 4, 8, 6};

        // display original values
        System.out.printf("Original values: %s%n", Arrays.asList(values));

        // sort values in ascending order with streams
        System.out.printf("Sorted values: %s%n",
                Arrays.stream(values)
                        .sorted()
                        .collect(Collectors.toList()));

        // values greater than 4
        List<Integer> greaterThan4 =
                Arrays.stream(values)
                        .filter(value -> value > 4)
                        .collect(Collectors.toList());
        System.out.printf("Values greater than 4: %s%n", greaterThan4);

        // filter values greater than 4 then sort the results
        System.out.printf("Sorted values greater than 4: %s%n",
                Arrays.stream(values)
                        .filter(value -> value > 4)
                        .sorted()
                        .collect(Collectors.toList()));

        // greaterThan4 List sorted with streams
        System.out.printf("Values greater than 4 (ascending with streams): %s%n",
                greaterThan4.stream()
                        .sorted()
                        .collect(Collectors.toList()));
    }
/*
Original values: [2, 9, 5, 0, 3, 7, 1, 4, 8, 6]
Sorted values: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
Values greater than 4: [9, 5, 7, 8, 6]
Sorted values greater than 4: [5, 6, 7, 8, 9]
Values greater than 4 (ascending with streams): [5, 6, 7, 8, 9]
 */
}
