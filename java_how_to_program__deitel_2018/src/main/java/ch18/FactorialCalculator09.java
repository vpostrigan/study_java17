package ch18;

// Fig. 18.9: FactorialCalculator.java
// Iterative factorial method.

public class FactorialCalculator09 {

    // iterative declaration of method factorial
    public static long factorial(long number) {
        long result = 1;

        // iteratively calculate factorial
        for (long i = number; i >= 1; i--) {
            result *= i;
        }

        return result;
    }

    public static void main(String[] args) {
        // calculate the factorials of 0 through 10
        for (int counter = 0; counter <= 10; counter++) {
            System.out.printf("%d! = %d%n", counter, factorial(counter));
        }
    }
/*
0! = 1
1! = 1
2! = 2
3! = 6
4! = 24
5! = 120
6! = 720
7! = 5040
8! = 40320
9! = 362880
10! = 3628800
 */
}
