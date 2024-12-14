package javase;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * https://www.youtube.com/@kamilbrzezinski8218
 *
 * https://www.youtube.com/watch?v=zNaUasfC84Y
 * Modern Java - Top Features of Java 9 to 17
 */
public class TopFeaturesOfJava9To17 {

    public static void main(String[] args) {
        // [1] Enhanced Switch
        enhancedSwitch("B");

        // [2] New instanceof
        AbstractFile abstractFile = new MusicFile();
        if (abstractFile instanceof MusicFile) {
            ((MusicFile) abstractFile).playMusic();
        }

        if (abstractFile instanceof MusicFile musicFile) {
            musicFile.playMusic();
        }

        // [3] Type Inference (var)
        String name = "name";
        var name2 = "name2";

        // [4] Records
        Person person = new Person("Test", 10);
        PersonRecord personRecord = new PersonRecord("Test2", 11);

        System.out.println(person.getAge());
        System.out.println(personRecord.age());

        // [5] Text Blocks
        String json = "{\"name\":\"Test\"" +
                ",\"age\":10}";
        String json2 = """
                {
                    "name":"Test",
                    "age":10
                }
                """;
        System.out.println(json2);

        // [6] Sealed Classes

        // [7] of for Collections
        List<String> test = List.of("test");
        Set<String> test2 = Set.of("test2");
        Map<String, String> map = Map.of("k", "v", "k2", "v2");
        Map<String, String> map2 = Map.ofEntries(Map.entry("k", "v"), Map.entry("k2", "v2"));

        // [8] Meaningful NullPointerException
        String s = null;
        System.out.println(s.toString());
        // Exception in thread "main" java.lang.NullPointerException: Cannot invoke "String.toString()" because "s" is null
    }

    private static void enhancedSwitch(String name) {
        switch (name) {
            case "A":
            case "B":
            case "C":
                System.out.println("Enhanced Switch");
                break;
            case "D":
                System.out.println("Enhanced Switch2");
                break;
            default:
                System.out.println("Enhanced Switch3");
                break;
        }

        // new
        switch (name) {
            case "A", "B", "C" -> System.out.println("Enhanced Switch");
            case "D" -> System.out.println("Enhanced Switch2");
            default -> System.out.println("Enhanced Switch3");
        }
    }

    // //

    private static abstract class AbstractFile {
    }

    private static class MusicFile extends AbstractFile {
        public void playMusic() {
            System.out.println("playMusic");
        }
    }

    private static class VideoFile extends AbstractFile {
        public void playVideo() {
            System.out.println("playVideo");
        }
    }

    // //

    private static class Person {
        private final String name;
        private final int age;

        public Person(String name, int age) {
            this.name = name;
            this.age = age;
        }

        public String getName() {
            return name;
        }

        public int getAge() {
            return age;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Person person)) return false;
            return age == person.age && Objects.equals(name, person.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, age);
        }

        @Override
        public String toString() {
            return "Person{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    '}';
        }
    }

    private record PersonRecord(String name, int age) {
    }

    // //

    // [6] Sealed Classes

    private static sealed abstract class AudioFile permits MP3File, MP3File2, MP3File31 {
    }

    private static final class MP3File extends AudioFile {
    }

    private static non-sealed class MP3File2 extends AudioFile {
    }

    private static sealed class MP3File31 extends AudioFile permits MP3File32 {
    }

    private static final class MP3File32 extends MP3File31 {
    }

}
