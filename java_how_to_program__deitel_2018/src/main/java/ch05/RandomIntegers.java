package ch05;

import java.security.SecureRandom; // program uses class SecureRandom

// Fig. 5.6: RandomIntegers.java
// Shifted and scaled random integers.
public class RandomIntegers {

    public static void main(String[] args) {
        // randomNumbers object will produce secure random numbers
        SecureRandom randomNumbers = new SecureRandom();

        // loop 20 times
        for (int counter = 1; counter <= 20; counter++) {
            // pick random integer from 1 to 6
            int face = 1 + randomNumbers.nextInt(6);

            System.out.printf("%d  ", face); // display generated value

            // if counter is divisible by 5, start a new line of output
            if (counter % 5 == 0) {
                System.out.println();
            }
        }
    }
/*
4  4  2  5  1
4  6  1  1  1
5  1  6  1  2
6  1  4  5  3
 */
}
