package ch15.TextFileApps;

import java.io.FileNotFoundException;
import java.lang.SecurityException;
import java.util.Formatter;
import java.util.FormatterClosedException;
import java.util.NoSuchElementException;
import java.util.Scanner;

// Fig. 15.3: CreateTextFile.java
// Writing data to a sequential text file with class Formatter.
public class CreateTextFile {

    public static void main(String[] args) {
        Scanner input = new Scanner(System.in);
        System.out.printf("%s%n%s%n? ",
                "Enter account number, first name, last name and balance.",
                "Enter end-of-file indicator to end input.");

        // open clients.txt, output data to the file then close clients.txt
        try (Formatter output = new Formatter("clients.txt")) {
            while (input.hasNext()) { // loop until end-of-file indicator
                try {
                    // output new record to file; assumes valid input
                    output.format("%d %s %s %.2f%n", input.nextInt(),
                            input.next(), input.next(), input.nextDouble());
                } catch (NoSuchElementException elementException) {
                    System.err.println("Invalid input. Please try again.");
                    input.nextLine(); // discard input so user can try again
                }

                System.out.print("? ");
            }
        } catch (SecurityException | FileNotFoundException | FormatterClosedException e) {
            e.printStackTrace();
            System.exit(1); // terminate the program
        }
    }

}
