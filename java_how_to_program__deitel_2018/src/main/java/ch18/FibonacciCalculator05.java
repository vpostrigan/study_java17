package ch18;

import java.math.BigInteger;

// Fig. 18.5: FibonacciCalculator.java
// Recursive fibonacci method.
public class FibonacciCalculator05 {
    private static BigInteger TWO = BigInteger.valueOf(2);

    // recursive declaration of method fibonacci
    public static BigInteger fibonacci(BigInteger number) {
        if (number.equals(BigInteger.ZERO) || number.equals(BigInteger.ONE)) { // base cases
            return number;
        } else { // recursion step
            return fibonacci(number.subtract(BigInteger.ONE)).add(fibonacci(number.subtract(TWO)));
        }
    }

    public static void main(String[] args) {
        // displays the fibonacci values from 0-40
        for (int counter = 0; counter <= 40; counter++) {
            System.out.printf("Fibonacci of %d is: %d%n", counter, fibonacci(BigInteger.valueOf(counter)));
        }
    }
/*
Fibonacci of 0 is: 0
Fibonacci of 1 is: 1
Fibonacci of 2 is: 1
Fibonacci of 3 is: 2
Fibonacci of 4 is: 3
Fibonacci of 5 is: 5
Fibonacci of 6 is: 8
Fibonacci of 7 is: 13
Fibonacci of 8 is: 21
Fibonacci of 9 is: 34
Fibonacci of 10 is: 55
Fibonacci of 11 is: 89
Fibonacci of 12 is: 144
Fibonacci of 13 is: 233
Fibonacci of 14 is: 377
Fibonacci of 15 is: 610
Fibonacci of 16 is: 987
Fibonacci of 17 is: 1597
Fibonacci of 18 is: 2584
Fibonacci of 19 is: 4181
Fibonacci of 20 is: 6765
Fibonacci of 21 is: 10946
Fibonacci of 22 is: 17711
Fibonacci of 23 is: 28657
Fibonacci of 24 is: 46368
Fibonacci of 25 is: 75025
Fibonacci of 26 is: 121393
Fibonacci of 27 is: 196418
Fibonacci of 28 is: 317811
Fibonacci of 29 is: 514229
Fibonacci of 30 is: 832040
Fibonacci of 31 is: 1346269
Fibonacci of 32 is: 2178309
Fibonacci of 33 is: 3524578
Fibonacci of 34 is: 5702887
Fibonacci of 35 is: 9227465
Fibonacci of 36 is: 14930352
Fibonacci of 37 is: 24157817
Fibonacci of 38 is: 39088169
Fibonacci of 39 is: 63245986
Fibonacci of 40 is: 102334155
 */
}
