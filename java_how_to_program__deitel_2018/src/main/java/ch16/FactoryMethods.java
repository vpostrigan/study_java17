package ch16;

import java.util.List;
import java.util.Map;
import java.util.Set;

// Fig. 16.20: FactoryMethods.java
// Java SE 9 collection factory methods.
public class FactoryMethods {

    public static void main(String[] args) {
        // create a List
        List<String> colorList = List.of("red", "orange", "yellow",
                "green", "blue", "indigo", "violet");
        System.out.printf("colorList: %s%n%n", colorList);

        // create a Set
        Set<String> colorSet = Set.of("red", "orange", "yellow",
                "green", "blue", "indigo", "violet");
        System.out.printf("colorSet: %s%n%n", colorSet);

        // create a Map using method "of"
        Map<String, Integer> dayMap = Map.of("Monday", 1, "Tuesday", 2,
                "Wednesday", 3, "Thursday", 4, "Friday", 5, "Saturday", 6, "Sunday", 7);
        System.out.printf("dayMap: %s%n%n", dayMap);

        // create a Map using method "ofEntries" for more than 10 pairs
        Map<String, Integer> daysPerMonthMap = Map.ofEntries(
                Map.entry("January", 31),
                Map.entry("February", 28),
                Map.entry("March", 31),
                Map.entry("April", 30),
                Map.entry("May", 31),
                Map.entry("June", 30),
                Map.entry("July", 31),
                Map.entry("August", 31),
                Map.entry("September", 30),
                Map.entry("October", 31),
                Map.entry("November", 30),
                Map.entry("December", 31)
        );
        System.out.printf("monthMap: %s%n", daysPerMonthMap);
    }

}
/*
colorList: [red, orange, yellow, green, blue, indigo, violet]

colorSet: [indigo, green, yellow, blue, violet, orange, red]

dayMap: {Sunday=7, Monday=1, Thursday=4, Tuesday=2, Saturday=6, Wednesday=3, Friday=5}

monthMap: {July=31, February=28, August=31, April=30, October=31, September=30, November=30, March=31, May=31, December=31, January=31, June=30}
*/
