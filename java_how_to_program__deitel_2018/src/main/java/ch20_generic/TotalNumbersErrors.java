package ch20_generic;

import java.util.ArrayList;
import java.util.List;

// TotalNumbersErrors.java
// Totaling the numbers in a List<Number>.
public class TotalNumbersErrors {

    public static void main(String[] args) {
        // create, initialize and output List of Numbers containing
        // both Integers and Doubles, then display total of the elements
        Integer[] integers = {1, 2, 3, 4};
        List<Integer> integerList = new ArrayList<>();

        for (Integer element : integers) {
            integerList.add(element); // place each number in numberList
        }

        System.out.printf("numberList contains: %s%n", integerList);
        System.out.printf("Total of the elements in numberList: %.1f%n", sum(integerList));
    }

    // calculate total of List elements
    public static double sum(List<? extends Number> list) {
        double total = 0; // initialize total

        // calculate sum
        for (Number element : list) {
            total += element.doubleValue();
        }

        return total;
    }
/*
numberList contains: [1, 2, 3, 4]
Total of the elements in numberList: 10.0
 */
}
