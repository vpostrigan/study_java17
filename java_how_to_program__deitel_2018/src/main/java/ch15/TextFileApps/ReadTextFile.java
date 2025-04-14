package ch15.TextFileApps;

import java.io.IOException;
import java.lang.IllegalStateException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.NoSuchElementException;
import java.util.Scanner;

// Fig. 15.6: ReadTextFile.java
// This program reads a text file and displays each record.
public class ReadTextFile {

    public static void main(String[] args) {
        // open clients.txt, read its contents and close the file
        try (Scanner input = new Scanner(Paths.get("clients.txt"))) {
            System.out.printf("%-10s%-12s%-12s%10s%n", "Account", "First Name", "Last Name", "Balance");

            // read record from file
            while (input.hasNext()) { // while there is more to read
                // display record contents
                System.out.printf("%-10d%-12s%-12s%10.2f%n", input.nextInt(),
                        input.next(), input.next(), input.nextDouble());
            }
        } catch (IOException | NoSuchElementException | IllegalStateException e) {
            e.printStackTrace();
        }
    }

}
