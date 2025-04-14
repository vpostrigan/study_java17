package ch14;

import java.util.Scanner;

// Fig. 14.18: TokenTest.java
// Tokenizing with String method split
public class TokenTest {

    public static void main(String[] args) {
        // get sentence
        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter a sentence and press Enter");
        String sentence = scanner.nextLine();

        // process user sentence
        String[] tokens = sentence.split(" ");
        System.out.printf("Number of elements: %d\nThe tokens are:\n", tokens.length);

        for (String token : tokens) {
            System.out.println(token);
        }
    }
/*
Enter a sentence and press Enter
Qwerty Hello
Number of elements: 2
The tokens are:
Qwerty
Hello
 */
}
