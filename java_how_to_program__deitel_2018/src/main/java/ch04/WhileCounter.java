package ch04;

// Fig. 4.1: WhileCounter.java
// Counter-controlled iteration with the while iteration statement.
public class WhileCounter {

    public static void main(String[] args) {
        int counter = 1; // declare and initialize control variable

        while (counter <= 10) { // loop-continuation condition
            System.out.printf("%d  ", counter);
            ++counter; // increment control variable
        }

        System.out.println();
    }
// 1  2  3  4  5  6  7  8  9  10
}
