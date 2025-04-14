package appK;

import appK.K_04BitRepresentation;

import java.util.Scanner;

// Fig. K.8: BitShift.java
// Using the bitwise shift operators.
public class K_08BitShift {

    public static void main(String[] args) {
        int choice = 0; // store operation type
        int input = 0; // store input integer
        int result = 0; // store operation result
        Scanner scanner = new Scanner(System.in); // create Scanner

        // continue execution until user exit
        while (true) {
            // get shift operation
            System.out.println("\n\nPlease choose the shift operation:");
            System.out.println("1--Left Shift (<<)");
            System.out.println("2--Signed Right Shift (>>)");
            System.out.println("3--Unsigned Right Shift (>>>)");
            System.out.println("4--Exit");
            choice = scanner.nextInt();

            // perform shift operation
            switch (choice) {
                case 1: // <<
                    System.out.println("Please enter an integer to shift:");
                    input = scanner.nextInt(); // get input integer
                    result = input << 1; // left shift one position
                    System.out.printf("\n%d << 1 = %d", input, result);
                    break;
                case 2: // >>
                    System.out.println("Please enter an integer to shift:");
                    input = scanner.nextInt(); // get input integer
                    result = input >> 1; // signed right shift one position
                    System.out.printf("\n%d >> 1 = %d", input, result);
                    break;
                case 3: // >>>
                    System.out.println("Please enter an integer to shift:");
                    input = scanner.nextInt(); // get input integer
                    result = input >>> 1; // unsigned right shift one position
                    System.out.printf("\n%d >>> 1 = %d", input, result);
                    break;
                case 4:
                default: // default operation is <<
                    System.exit(0); // exit application
            }

            // display input integer and result in bits
            K_04BitRepresentation.display(input);
            K_04BitRepresentation.display(result);
        }
    }

}
