package ch34;

import java.util.Scanner;

// Keypad.java
// Represents the keypad of the ATM program uses Scanner to obtain user input
public class Keypad {
    private Scanner input; // reads data from the command line

    public Keypad() {
        input = new Scanner(System.in);
    }

    // return an integer value entered by user
    public int getInput() {
        return input.nextInt(); // we assume that user enters an integer
    }
}
