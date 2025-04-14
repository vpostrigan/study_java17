package appK;

import java.util.Scanner;

// Fig. K.4: MiscBitOps.java
// Using the bitwise operators.
public class K_04MiscBitOps {

    public static void main(String[] args) {
        int choice = 0; // store operation type
        int first = 0; // store first input integer
        int second = 0; // store second input integer
        int result = 0; // store operation result
        Scanner scanner = new Scanner(System.in); // create Scanner

        // continue execution until user exit
        while (true) {
            // get selected operation
            System.out.println("\n\nPlease choose the operation:");
            System.out.printf("%s%s", "1--AND\n2--Inclusive OR\n", "3--Exclusive OR\n4--Complement\n5--Exit\n");
            choice = scanner.nextInt();

            // perform bitwise operation
            switch (choice) {
                case 1: // AND
                    System.out.print("Please enter two integers:");
                    first = scanner.nextInt(); // get first input integer
                    K_04BitRepresentation.display(first);
                    second = scanner.nextInt(); // get second input integer
                    K_04BitRepresentation.display(second);
                    result = first & second; // perform bitwise AND
                    System.out.printf("\n\n%d & %d = %d", first, second, result);
                    K_04BitRepresentation.display(result);
                    break;
                case 2: // Inclusive OR
                    System.out.print("Please enter two integers:");
                    first = scanner.nextInt(); // get first input integer
                    K_04BitRepresentation.display(first);
                    second = scanner.nextInt(); // get second input integer
                    K_04BitRepresentation.display(second);
                    result = first | second; // perform bitwise inclusive OR
                    System.out.printf("\n\n%d | %d = %d", first, second, result);
                    K_04BitRepresentation.display(result);
                    break;
                case 3: // Exclusive OR
                    System.out.print("Please enter two integers:");
                    first = scanner.nextInt(); // get first input integer
                    K_04BitRepresentation.display(first);
                    second = scanner.nextInt(); // get second input integer
                    K_04BitRepresentation.display(second);
                    result = first ^ second; // perform bitwise exclusive OR
                    System.out.printf("\n\n%d ^ %d = %d", first, second, result);
                    K_04BitRepresentation.display(result);
                    break;
                case 4: // Complement
                    System.out.print("Please enter one integer:");
                    first = scanner.nextInt(); // get input integer
                    K_04BitRepresentation.display(first);
                    result = ~first; // perform bitwise complement on first
                    System.out.printf("\n\n~%d = %d", first, result);
                    K_04BitRepresentation.display(result);
                    break;
                case 5:
                default:
                    System.exit(0); // exit application
            }
        }
    }

}
