package appK;

import java.util.Scanner;

// Fig. K.2: PrintBits.java
// Printing an unsigned integer in bits.
public class K_02PrintBits {

    public static void main(String[] args) {
        // get input integer
        Scanner scanner = new Scanner(System.in);
        System.out.println("Please enter an integer:");
        int input = scanner.nextInt();

        K_04BitRepresentation.display(input);
    }
/*
Please enter an integer:
54

The integer in bits is:
00000000 00000000 00000000 00110110
 */
}

