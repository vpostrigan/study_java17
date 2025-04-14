package appL;

// Fig. L.1: BreakLabelTest.java
// Labeled break statement exiting a nested for statement.
public class BreakLabelTest {

    public static void main(String[] args) {
        stop:
        // labeled block
        {
            // count 10 rows
            for (int row = 1; row <= 10; row++) {
                // count 5 columns
                for (int column = 1; column <= 5; column++) {
                    if (row == 5) { // if row is 5,
                        break stop;  // jump to end of stop block
                    }

                    System.out.print("* ");
                }

                System.out.println(); // outputs a newline
            }

            // following line is skipped
            System.out.println("\nLoops terminated normally");
        }
    }
/*
 * * * * *
 * * * * *
 * * * * *
 * * * * *
 */
}
