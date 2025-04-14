package ch17;

import java.security.SecureRandom;
import java.util.stream.Collectors;

// Fig. 17.8: RandomIntegers.java
// Shifted and scaled random integers.
public class RandomIntegers {

    public static void main(String[] args) {
        SecureRandom randomNumbers = new SecureRandom();

        // display 10 random integers on separate lines
        System.out.println("Random numbers on separate lines:");
        randomNumbers.ints(10, 1, 7)
                .forEach(System.out::println);

        // display 10 random integers on the same line
        String numbers = randomNumbers.ints(10, 1, 7)
                .mapToObj(String::valueOf)
                .collect(Collectors.joining(" "));
        System.out.printf("%nRandom numbers on one line: %s%n", numbers);
    }
/*
Random numbers on separate lines:
1
3
2
4
3
2
2
3
1
6

Random numbers on one line: 6 6 2 2 2 3 2 3 5 1
 */
}
