package javase;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class LambdaOptimization {

    public static void main(String[] args) {
        // Old
        Set<String> urls = new HashSet<>();
        urls.add("test1");
        urls.add("test2");
        urls.add("test3");

        Iterator<String> iterator = urls.iterator();
        while (iterator.hasNext()) {
            final String url0 = iterator.next();
            if (url0.endsWith("1")) {
                iterator.remove();
            }
        }
        System.out.println(urls);

        // New
        urls = new HashSet<>();
        urls.add("test1");
        urls.add("test2");
        urls.add("test3");

        urls.removeIf(s -> s.endsWith("1"));
        System.out.println(urls);
    }

}
