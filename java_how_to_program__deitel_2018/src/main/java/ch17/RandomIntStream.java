package ch17;

import java.security.SecureRandom;
import java.util.function.Function;
import java.util.stream.Collectors;

// Fig. 17.24: RandomIntStream.java
// Rolling a die 1,000,000 times with streams
public class RandomIntStream {

    public static void main(String[] args) {
        SecureRandom random = new SecureRandom();

        // roll a die 1,000,000 times and summarize the results
        System.out.printf("%-6s%s%n", "Face", "Frequency");
        random.ints(1_000_000, 1, 7)
                .boxed()
                .collect(Collectors.groupingBy(Function.identity(),
                        Collectors.counting()))
                .forEach((face, frequency) ->
                        System.out.printf("%-6d%d%n", face, frequency));
    }
/*
Face  Frequency
1     166944
2     166765
3     166610
4     167085
5     166242
6     166354
 */
}
