package ch19_swing;

import java.security.SecureRandom;
import java.util.Arrays;

// Fig. 19.4: SelectionSortTest.java
// Sorting an array with selection sort.
public class SelectionSortTest {

    // sort array using selection sort
    public static void selectionSort(int[] data) {
        // loop over data.length - 1 elements
        for (int i = 0; i < data.length - 1; i++) {
            int smallest = i; // first index of remaining array

            // loop to find index of smallest element
            for (int index = i + 1; index < data.length; index++) {
                if (data[index] < data[smallest]) {
                    smallest = index;
                }
            }

            swap(data, i, smallest); // swap smallest element into position
            printPass(data, i + 1, smallest); // output pass of algorithm
        }
    }

    // helper method to swap values in two elements
    private static void swap(int[] data, int first, int second) {
        int temporary = data[first]; // store first in temporary
        data[first] = data[second]; // replace first with second
        data[second] = temporary; // put temporary in second
    }

    // print a pass of the algorithm
    private static void printPass(int[] data, int pass, int index) {
        System.out.printf("after pass %2d: ", pass);

        // output elements till selected item
        for (int i = 0; i < index; i++) {
            System.out.printf("%d  ", data[i]);
        }

        System.out.printf("%d* ", data[index]); // indicate swap

        // finish outputting array
        for (int i = index + 1; i < data.length; i++) {
            System.out.printf("%d  ", data[i]);
        }

        System.out.printf("%n               "); // for alignment

        // indicate amount of array that's sorted
        for (int j = 0; j < pass; j++) {
            System.out.print("--  ");
        }
        System.out.println();
    }

    public static void main(String[] args) {
        SecureRandom generator = new SecureRandom();

        // create unordered array of 10 random ints
        int[] data = generator.ints(10, 10, 91).toArray();

        System.out.printf("Unsorted array: %s%n%n", Arrays.toString(data));
        selectionSort(data); // sort array
        System.out.printf("%nSorted array: %s%n", Arrays.toString(data));
    }
/*
Unsorted array: [82, 55, 38, 60, 20, 12, 82, 22, 80, 89]

after pass  1: 12  55  38  60  20  82* 82  22  80  89
               --
after pass  2: 12  20  38  60  55* 82  82  22  80  89
               --  --
after pass  3: 12  20  22  60  55  82  82  38* 80  89
               --  --  --
after pass  4: 12  20  22  38  55  82  82  60* 80  89
               --  --  --  --
after pass  5: 12  20  22  38  55* 82  82  60  80  89
               --  --  --  --  --
after pass  6: 12  20  22  38  55  60  82  82* 80  89
               --  --  --  --  --  --
after pass  7: 12  20  22  38  55  60  80  82  82* 89
               --  --  --  --  --  --  --
after pass  8: 12  20  22  38  55  60  80  82* 82  89
               --  --  --  --  --  --  --  --
after pass  9: 12  20  22  38  55  60  80  82  82* 89
               --  --  --  --  --  --  --  --  --

Sorted array: [12, 20, 22, 38, 55, 60, 80, 82, 82, 89]
 */
}
