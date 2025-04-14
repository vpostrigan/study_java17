package ch16;

import java.util.List;
import java.util.Arrays;
import java.util.Collections;

// Fig. 16.7: Sort2.java
// Using a Comparator object with method sort.
public class Sort2 {

    public static void main(String[] args) {
        String[] suits = {"Hearts", "Diamonds", "Clubs", "Spades"};

        // Create and display a list containing the suits array elements
        List<String> list = Arrays.asList(suits); // create List
        System.out.printf("Unsorted array elements: %s%n", list);

        // sort in descending order using a comparator
        Collections.sort(list, Collections.reverseOrder());
        System.out.printf("Sorted list elements: %s%n", list);
    }

}
/*
Unsorted array elements: [Hearts, Diamonds, Clubs, Spades]
Sorted list elements: [Spades, Hearts, Diamonds, Clubs]
 */