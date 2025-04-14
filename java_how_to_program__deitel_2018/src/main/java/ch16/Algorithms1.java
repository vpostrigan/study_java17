package ch16;

import java.util.List;
import java.util.Arrays;
import java.util.Collections;

// Fig. 16.11: Algorithms1.java
// Collections methods reverse, fill, copy, max and min.
public class Algorithms1 {

    public static void main(String[] args) {
        // create and display a List<Character>
        Character[] letters = {'P', 'C', 'M'};
        List<Character> list = Arrays.asList(letters); // get List
        System.out.println("list contains: ");
        output(list);

        // reverse and display the List<Character>
        Collections.reverse(list); // reverse order the elements
        System.out.printf("%nAfter calling reverse, list contains:%n");
        output(list);

        // create copyList from an array of 3 Characters
        Character[] lettersCopy = new Character[3];
        List<Character> copyList = Arrays.asList(lettersCopy);

        // copy the contents of list into copyList
        Collections.copy(copyList, list);
        System.out.printf("%nAfter copying, copyList contains:%n");
        output(copyList);

        // fill list with Rs
        Collections.fill(list, 'R');
        System.out.printf("%nAfter calling fill, list contains:%n");
        output(list);
    }

    // output List information
    private static void output(List<Character> listRef) {
        System.out.print("The list is: ");

        for (Character element : listRef) {
            System.out.printf("%s ", element);
        }

        System.out.printf("%nMax: %s", Collections.max(listRef));
        System.out.printf("  Min: %s%n", Collections.min(listRef));
    }
/*
list contains:
The list is: P C M
Max: P  Min: C

After calling reverse, list contains:
The list is: M C P
Max: P  Min: C

After copying, copyList contains:
The list is: M C P
Max: P  Min: C

After calling fill, list contains:
The list is: R R R
Max: R  Min: R
 */
}
