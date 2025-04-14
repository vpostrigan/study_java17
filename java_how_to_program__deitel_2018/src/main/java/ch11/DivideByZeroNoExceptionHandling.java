package ch11;

import java.util.Scanner;

// Fig. 11.2: DivideByZeroNoExceptionHandling.java
// Integer division without exception handling.
public class DivideByZeroNoExceptionHandling {

    // demonstrates throwing an exception when a divide-by-zero occurs
    public static int quotient(int numerator, int denominator) {
        return numerator / denominator; // possible division by zero
    }

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        System.out.print("Please enter an integer numerator: ");
        int numerator = scanner.nextInt();
        System.out.print("Please enter an integer denominator: ");
        int denominator = scanner.nextInt();

        int result = quotient(numerator, denominator);
        System.out.printf("%nResult: %d / %d = %d%n", numerator, denominator, result);
    }
/*
Please enter an integer numerator: 54
Please enter an integer denominator: 5

Result: 54 / 5 = 10
 */
}
