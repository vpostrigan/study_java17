package appK;

// Fig K.5: BitRepresentation.java
// Utility class that displays bit representation of an integer.
public class K_04BitRepresentation {

    // display bit representation of specified int value
    static void display(int value) {
        System.out.printf("\nBit representation of %d is: \n", value);

        // create int value with 1 in leftmost bit and 0s elsewhere
        int displayMask = 1 << 31;

        // for each bit display 0 or 1
        for (int bit = 1; bit <= 32; bit++) {
            // use displayMask to isolate bit
            System.out.print((value & displayMask) == 0 ? '0' : '1');

            value <<= 1; // shift value one position to left

            if (bit % 8 == 0) {
                System.out.print(' '); // display space every 8 bits
            }
        }
    }

}
