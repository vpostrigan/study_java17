package baeldung_com;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

/**
 * https://www.baeldung.com/java-enumset
 * An EnumSet is a specialized Set collection to work with enum classes
 */
public class JavaUtil_EnumSet {

    public enum Color {
        RED, YELLOW, GREEN, BLUE, BLACK, WHITE
    }

    public static void main(String[] args) {
        // create
        EnumSet<Color> all = EnumSet.allOf(Color.class);
        System.out.println(all); // [RED, YELLOW, GREEN, BLUE, BLACK, WHITE]
        EnumSet<Color> none = EnumSet.noneOf(Color.class);
        System.out.println(none); // []
        EnumSet<Color> range = EnumSet.range(Color.YELLOW, Color.BLUE);
        System.out.println(range); // [YELLOW, GREEN, BLUE]
        EnumSet<Color> complementOf = EnumSet.complementOf(EnumSet.of(Color.BLACK, Color.WHITE));
        System.out.println(complementOf); // [RED, YELLOW, GREEN, BLUE]
        EnumSet<Color> copyOf = EnumSet.copyOf(EnumSet.of(Color.BLACK, Color.WHITE));
        System.out.println(copyOf); // [BLACK, WHITE]

        List<Color> colorsList = new ArrayList<>();
        colorsList.add(Color.RED);
        EnumSet<Color> listCopy = EnumSet.copyOf(colorsList);
        System.out.println(listCopy); // [RED]

        // operations
        EnumSet<Color> set = EnumSet.noneOf(Color.class);
        set.add(Color.RED);
        set.add(Color.YELLOW);

        // Check if the collection contains a specific element:
        System.out.println("set.contains(Color.RED): " + set.contains(Color.RED));

        // Iterate over the elements:
        set.forEach(System.out::println);

        // Or simply remove elements:
        System.out.println("set.remove(Color.RED): " + set.remove(Color.RED));
    }

}
