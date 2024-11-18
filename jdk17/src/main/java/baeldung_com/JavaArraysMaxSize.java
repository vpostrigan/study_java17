package baeldung_com;

/**
 * https://www.baeldung.com/java-arrays-max-size
 */
public class JavaArraysMaxSize {

    public static void main(String[] args) {
        // Java program can only allocate an array up to a certain size
        // approximate index value can be 2^31 – 1
        // array can theoretically hold 2,147,483,647 elements

        // VM options:  -Xms9G -Xmx9G

        System.out.println("totalMemory: " + (Runtime.getRuntime().totalMemory() / 1024.0 / 1024.0) + " MB");
        for (int i = 2; i >= 0; i--) {
            try {
                int[] arr = new int[Integer.MAX_VALUE - i];
                System.out.println("Max-Size : " + arr.length);
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
        // java.lang.OutOfMemoryError: Requested array size exceeds VM limit
        //         at com.example.demo.ArraySizeCheck.main(ArraySizeCheck.java:8)

        // maximum size as Integer.MAX_VALUE – 8 to make it work with all the JDK versions and implementations
    }

}
