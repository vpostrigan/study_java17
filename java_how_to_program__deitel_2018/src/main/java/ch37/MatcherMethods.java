package ch37;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

// Fig. 37.1: MatcherMethods.java
// Java 9's new Matcher methods.
public class MatcherMethods {

    public static void main(String[] args) {
        String sentence = "a man a plan a canal panama";

        System.out.printf("sentence: %s%n", sentence);

        // using Matcher methods appendReplacement and appendTail
        Pattern pattern = Pattern.compile("an"); // regex to match

        // match regular expression to String and replace each match with uppercase letters
        Matcher matcher = pattern.matcher(sentence);

        // used to rebuild String
        StringBuilder sb = new StringBuilder();

        // append text to builder; convert each match to uppercase
        while (matcher.find()) {
            matcher.appendReplacement(sb, matcher.group().toUpperCase());
        }

        // append the remainder of the original String to builder
        matcher.appendTail(sb);
        System.out.printf("%nAfter appendReplacement/appendTail: %s%n", sb);
        // After appendReplacement/appendTail: a mAN a plAN a cANal pANama

        // using Matcher method replaceFirst
        matcher.reset(); // reset matcher to its initial state
        System.out.printf("%nBefore replaceFirst: %s%n", sentence);
        String result = matcher.replaceFirst(m -> m.group().toUpperCase());
        System.out.printf("After replaceFirst: %s%n", result);
        // Before replaceFirst: a man a plan a canal panama
        // After replaceFirst: a mAN a plan a canal panama

        // using Matcher method replaceAll
        matcher.reset(); // reset matcher to its initial state
        System.out.printf("%nBefore replaceAll: %s%n", sentence);
        result = matcher.replaceAll(m -> m.group().toUpperCase());
        System.out.printf("After replaceAll: %s%n", result);
        // Before replaceAll: a man a plan a canal panama
        // After replaceAll: a mAN a plAN a cANal pANama

        // using method results to get a Stream<MatchResult>
        System.out.printf("%nUsing Matcher method results:%n");
        pattern = Pattern.compile("\\w+"); // regular expression to match
        matcher = pattern.matcher(sentence);
        System.out.printf("The number of words is: %d%n", matcher.results().count());
        // The number of words is: 7

        matcher.reset(); // reset matcher to its initial state
        System.out.printf("Average characters per word is: %f%n",
                matcher.results()
                        .mapToInt(m -> m.group().length())
                        .average().orElse(0));
        // Average characters per word is: 3.000000
    }

}
