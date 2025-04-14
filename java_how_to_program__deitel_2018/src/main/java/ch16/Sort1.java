package ch16;

import java.util.List;
import java.util.Arrays;
import java.util.Collections;

// Fig. 16.6: Sort1.java
// Collections method sort.
public class Sort1 {

    public static void main(String[] args) {
        String[] suits = {"Hearts", "Diamonds", "Clubs", "Spades"};

        // Create and display a list containing the suits array elements
        List<String> list = Arrays.asList(suits);
        System.out.printf("Unsorted array elements: %s%n", list);

        Collections.sort(list); // sort ArrayList
        System.out.printf("Sorted array elements: %s%n", list);
    }

}
/*
Unsorted array elements: [Hearts, Diamonds, Clubs, Spades]
Sorted array elements: [Clubs, Diamonds, Hearts, Spades]
 */