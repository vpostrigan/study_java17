package ch17.fig17_22;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

// Fig. 17.22: StreamOfLines.java
// Counting word occurrences in a text file.
public class StreamOfLines {

    public static void main(String[] args) throws IOException {
        // Regex that matches one or more consecutive whitespace characters
        Pattern pattern = Pattern.compile("\\s+");

        // count occurrences of each word in a Stream<String> sorted by word
        Map<String, Long> wordCounts =
                Files.lines(Paths.get("Chapter2Paragraph.txt"))
                        .flatMap(line -> pattern.splitAsStream(line))
                        .collect(Collectors.groupingBy(String::toLowerCase,
                                TreeMap::new, Collectors.counting()));

        // display the words grouped by starting letter
        wordCounts.entrySet()
                .stream()
                .collect(
                        Collectors.groupingBy(entry -> entry.getKey().charAt(0),
                                TreeMap::new, Collectors.toList()))
                .forEach((letter, wordList) -> {
                    System.out.printf("%n%C%n", letter);
                    wordList.stream().forEach(word -> System.out.printf(
                            "%13s: %d%n", word.getKey(), word.getValue()));
                });
    }
/*

A
            a: 2
          and: 3
  application: 2
   arithmetic: 1

B
        begin: 1

C
   calculates: 1
 calculations: 1
      chapter: 1
    chapter's: 1
  commandline: 1
     compares: 1
   comparison: 1
      compile: 1
     computer: 1

D
    decisions: 1
 demonstrates: 1
      display: 1
     displays: 2

E
      example: 1
     examples: 1

F
          for: 1
         from: 1

H
          how: 2

I
       inputs: 1
     instruct: 1
   introduces: 1

J
         java: 1
          jdk: 1

L
         last: 1
        later: 1
        learn: 1

M
         make: 1
     messages: 2

N
      numbers: 2

O
      obtains: 1
           of: 1
           on: 1
       output: 1

P
      perform: 1
      present: 1
      program: 1
  programming: 1
     programs: 2

R
       result: 1
      results: 2
          run: 1

S
         save: 1
       screen: 1
         show: 1
          sum: 1

T
         that: 3
          the: 7
        their: 2
         then: 2
         this: 2
           to: 4
        tools: 1
          two: 2

U
          use: 2
         user: 1

W
           we: 2
         with: 1

Y
       you'll: 2
 */
}
