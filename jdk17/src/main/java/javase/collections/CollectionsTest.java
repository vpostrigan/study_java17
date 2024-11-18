package javase.collections;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CollectionsTest {

    public static void main(String[] args) {
        Map<String, String> map = new HashMap<>();
        map.put("a", "1");
        map.put("b", "2");
        map.put("c", "1");
        map.put("d", "3");
        map.values().removeAll(Collections.singleton("1"));
        System.out.println(map.toString());
        // {b=2, d=3}

        // //

        Map<String, Integer> citiesWithCodes = new HashMap<>();
        citiesWithCodes.put("Berlin", 49);
        citiesWithCodes.put("Bern", 41);
        citiesWithCodes.put("Frankfurt", 49);
        citiesWithCodes.put("Hamburg", 49);
        citiesWithCodes.put("Cologne", 49);
        citiesWithCodes.put("Salzburg", 43);
        citiesWithCodes.put("Vienna", 43);
        citiesWithCodes.put("Zurich", 41);
        citiesWithCodes.put("Interlaken", 41);

        Map<Integer, List<String>> result = citiesWithCodes.entrySet().stream()
                .collect(Collectors.groupingBy(
                        Map.Entry::getValue,
                        Collectors.mapping(Map.Entry::getKey, Collectors.toList())));
        System.out.println(result);
        // {49=[Frankfurt, Berlin, Hamburg, Cologne], 41=[Bern, Zurich, Interlaken], 43=[Vienna, Salzburg]}

        Map<String, String> citiesWithCodes2 = new HashMap<>();
        citiesWithCodes2.put("Berlin", "49");
        citiesWithCodes2.put("Frankfurt", "49");
        citiesWithCodes2.put("Bern", "41");
        citiesWithCodes2.put("Cologne", "49");
        citiesWithCodes2.put("Salzburg", "43");
        citiesWithCodes2.put("Vienna", "43");
        citiesWithCodes2.put("Zurich", "41");
        citiesWithCodes2.put("Hamburg", "49");
        citiesWithCodes2.put("Interlaken", "41");

        Map<String, List<String>> result2 = citiesWithCodes2.entrySet().stream()
                .collect(Collectors.groupingBy(
                        Map.Entry::getValue,
                        Collectors.mapping(Map.Entry::getKey, Collectors.toList())));
        System.out.println(result2);
        // {49=[Frankfurt, Berlin, Hamburg, Cologne], 41=[Bern, Zurich, Interlaken], 43=[Vienna, Salzburg]}
    }

}
