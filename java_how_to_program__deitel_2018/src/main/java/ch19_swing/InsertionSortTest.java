package ch19_swing;

import java.security.SecureRandom;
import java.util.Arrays;

// Fig. 19.5: InsertionSortTest.java
// Sorting an array with insertion sort.
public class InsertionSortTest {

    public static void main(String[] args) {
        SecureRandom generator = new SecureRandom();

        // create unordered array of 10 random ints
        int[] data = generator.ints(10, 10, 91).toArray();

        System.out.printf("Unsorted array: %s%n%n", Arrays.toString(data));
        insertionSort(data); // sort array
        System.out.printf("%nSorted array: %s%n", Arrays.toString(data));
    }

    // sort array using insertion sort
    public static void insertionSort(int[] data) {
        // loop over data.length - 1 elements
        for (int next = 1; next < data.length; next++) {
            int insert = data[next]; // value to insert
            int moveItem = next; // location to place element

            // search for place to put current element
            while (moveItem > 0 && data[moveItem - 1] > insert) {
                // shift element right one slot
                data[moveItem] = data[moveItem - 1];
                moveItem--;
            }

            data[moveItem] = insert; // place inserted element
            printPass(data, next, moveItem); // output pass of algorithm
        }
    }

    // print a pass of the algorithm
    public static void printPass(int[] data, int pass, int index) {
        System.out.printf("after pass %2d: ", pass);

        // output elements till swapped item
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
        for (int i = 0; i <= pass; i++) {
            System.out.print("--  ");
        }
        System.out.println();
    }
/*
Unsorted array: [88, 54, 14, 67, 88, 69, 59, 82, 62, 15]

after pass  1: 54* 88  14  67  88  69  59  82  62  15
               --  --
after pass  2: 14* 54  88  67  88  69  59  82  62  15
               --  --  --
after pass  3: 14  54  67* 88  88  69  59  82  62  15
               --  --  --  --
after pass  4: 14  54  67  88  88* 69  59  82  62  15
               --  --  --  --  --
after pass  5: 14  54  67  69* 88  88  59  82  62  15
               --  --  --  --  --  --
after pass  6: 14  54  59* 67  69  88  88  82  62  15
               --  --  --  --  --  --  --
after pass  7: 14  54  59  67  69  82* 88  88  62  15
               --  --  --  --  --  --  --  --
after pass  8: 14  54  59  62* 67  69  82  88  88  15
               --  --  --  --  --  --  --  --  --
after pass  9: 14  15* 54  59  62  67  69  82  88  88
               --  --  --  --  --  --  --  --  --  --

Sorted array: [14, 15, 54, 59, 62, 67, 69, 82, 88, 88]
 */
}
