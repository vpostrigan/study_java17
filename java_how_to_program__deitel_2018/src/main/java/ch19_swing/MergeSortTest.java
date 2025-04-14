package ch19_swing;

import java.security.SecureRandom;
import java.util.Arrays;

// Fig. 19.6: MergeSortTest.java
// Sorting an array with merge sort.
public class MergeSortTest {

    // calls recursive sortArray method to begin merge sorting
    public static void mergeSort(int[] data) {
        sortArray(data, 0, data.length - 1); // sort entire array
    }

    // splits array, sorts subarrays and merges subarrays into sorted array
    private static void sortArray(int[] data, int low, int high) {
        // test base case; size of array equals 1
        if ((high - low) >= 1) { // if not base case
            int middle1 = (low + high) / 2; // calculate middle of array
            int middle2 = middle1 + 1; // calculate next element over

            // output split step
            System.out.printf("split:   %s%n", subarrayString(data, low, high));
            System.out.printf("         %s%n", subarrayString(data, low, middle1));
            System.out.printf("         %s%n%n", subarrayString(data, middle2, high));

            // split array in half; sort each half (recursive calls)
            sortArray(data, low, middle1); // first half of array
            sortArray(data, middle2, high); // second half of array

            // merge two sorted arrays after split calls return
            merge(data, low, middle1, middle2, high);
        }
    }

    // merge two sorted subarrays into one sorted subarray
    private static void merge(int[] data, int left, int middle1, int middle2, int right) {

        int leftIndex = left; // index into left subarray
        int rightIndex = middle2; // index into right subarray
        int combinedIndex = left; // index into temporary working array
        int[] combined = new int[data.length]; // working array

        // output two subarrays before merging
        System.out.printf("merge:   %s%n", subarrayString(data, left, middle1));
        System.out.printf("         %s%n", subarrayString(data, middle2, right));

        // merge arrays until reaching end of either
        while (leftIndex <= middle1 && rightIndex <= right) {
            // place smaller of two current elements into result
            // and move to next space in arrays
            if (data[leftIndex] <= data[rightIndex]) {
                combined[combinedIndex++] = data[leftIndex++];
            } else {
                combined[combinedIndex++] = data[rightIndex++];
            }
        }

        // if left array is empty
        if (leftIndex == middle2) {
            // copy in rest of right array
            while (rightIndex <= right) {
                combined[combinedIndex++] = data[rightIndex++];
            }
        } else { // right array is empty
            // copy in rest of left array
            while (leftIndex <= middle1) {
                combined[combinedIndex++] = data[leftIndex++];
            }
        }

        // copy values back into original array
        for (int i = left; i <= right; i++) {
            data[i] = combined[i];
        }

        // output merged array
        System.out.printf("         %s%n%n", subarrayString(data, left, right));
    }

    // method to output certain values in array
    private static String subarrayString(int[] data, int low, int high) {
        StringBuilder temporary = new StringBuilder();
        // output spaces for alignment
        for (int i = 0; i < low; i++) {
            temporary.append("   ");
        }
        // output elements left in array
        for (int i = low; i <= high; i++) {
            temporary.append(" " + data[i]);
        }
        return temporary.toString();
    }

    public static void main(String[] args) {
        SecureRandom generator = new SecureRandom();

        // create unordered array of 10 random ints
        int[] data = generator.ints(10, 10, 91).toArray();

        System.out.printf("Unsorted array: %s%n%n", Arrays.toString(data));
        mergeSort(data); // sort array
        System.out.printf("Sorted array: %s%n", Arrays.toString(data));
    }
/*
Unsorted array: [13, 72, 70, 41, 78, 18, 49, 75, 28, 25]

split:    13 72 70 41 78 18 49 75 28 25
          13 72 70 41 78
                         18 49 75 28 25

split:    13 72 70 41 78
          13 72 70
                   41 78

split:    13 72 70
          13 72
                70

split:    13 72
          13
             72

merge:    13
             72
          13 72

merge:    13 72
                70
          13 70 72

split:             41 78
                   41
                      78

merge:             41
                      78
                   41 78

merge:    13 70 72
                   41 78
          13 41 70 72 78

split:                   18 49 75 28 25
                         18 49 75
                                  28 25

split:                   18 49 75
                         18 49
                               75

split:                   18 49
                         18
                            49

merge:                   18
                            49
                         18 49

merge:                   18 49
                               75
                         18 49 75

split:                            28 25
                                  28
                                     25

merge:                            28
                                     25
                                  25 28

merge:                   18 49 75
                                  25 28
                         18 25 28 49 75

merge:    13 41 70 72 78
                         18 25 28 49 75
          13 18 25 28 41 49 70 72 75 78

Sorted array: [13, 18, 25, 28, 41, 49, 70, 72, 75, 78]
 */
}
