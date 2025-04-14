package ch17;

import java.util.stream.IntStream;

// ModifiedToDisplayIntermediateOps

// Fig. 17.7: StreamFilterMapReduce.java
// Triple the even ints from 2 through 10 then sum them with IntStream.
public class StreamFilterMapReduce2 {

    public static void main(String[] args) {
        // sum the triples of the even integers from 2 through 10
        System.out.printf("Sum of the triples of the even ints from 2 through 10 is: %d%n",
                IntStream.rangeClosed(1, 10)
                        .filter(x -> {
                            System.out.printf("%nfilter: %d%n", x);
                            return x % 2 == 0;
                        })
                        .map(x -> {
                            System.out.println("map: " + x);
                            return x * 3;
                        })
                        .sum());
    }
/*
filter: 1

filter: 2
map: 2

filter: 3

filter: 4
map: 4

filter: 5

filter: 6
map: 6

filter: 7

filter: 8
map: 8

filter: 9

filter: 10
map: 10
Sum of the triples of the even ints from 2 through 10 is: 90
 */
}
