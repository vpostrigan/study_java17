package ch14;

import java.util.Arrays;

// Fig. 14.23: RegexSubstitution.java
// String methods replaceFirst, replaceAll and split.
public class RegexSubstitution {

    public static void main(String[] args) {
        String firstString = "This sentence ends in 5 stars *****";
        String secondString = "1, 2, 3, 4, 5, 6, 7, 8";

        System.out.printf("Original String 1: %s\n", firstString);

        // replace '*' with '^'
        firstString = firstString.replaceAll("\\*", "^");

        System.out.printf("^ substituted for *: %s\n", firstString);

        // replace 'stars' with 'carets'
        firstString = firstString.replaceAll("stars", "carets");

        System.out.printf("\"carets\" substituted for \"stars\": %s\n", firstString);

        // replace words with 'word'
        System.out.printf("Every word replaced by \"word\": %s\n\n", firstString.replaceAll("\\w+", "word"));

        System.out.printf("Original String 2: %s\n", secondString);

        // replace first three digits with 'digit'
        for (int i = 0; i < 3; i++) {
            secondString = secondString.replaceFirst("\\d", "digit");
        }

        System.out.printf("First 3 digits replaced by \"digit\" : %s\n", secondString);

        System.out.print("String split at commas: ");
        String[] results = secondString.split(",\\s*"); // split on commas
        System.out.println(Arrays.toString(results)); // display results
    }
/*
Original String 1: This sentence ends in 5 stars *****
^ substituted for *: This sentence ends in 5 stars ^^^^^
"carets" substituted for "stars": This sentence ends in 5 carets ^^^^^
Every word replaced by "word": word word word word word word ^^^^^

Original String 2: 1, 2, 3, 4, 5, 6, 7, 8
First 3 digits replaced by "digit" : digit, digit, digit, 4, 5, 6, 7, 8
String split at commas: [digit, digit, digit, 4, 5, 6, 7, 8]
 */
}
