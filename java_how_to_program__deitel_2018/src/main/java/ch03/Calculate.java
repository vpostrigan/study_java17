package ch03;

// Exercise 3.6: Calculate.java
// Calculate the sum of the integers from 1 to 10 
public class Calculate {

    public static void main(String[] args) {
        int sum = 0;
        int x = 1;

        while (x <= 10) { // while x is less than or equal to 10
            sum += x; // add x to sum
            ++x; // increment x
        }

        System.out.printf("The sum is: %d%n", sum); // The sum is: 55
    }

}
