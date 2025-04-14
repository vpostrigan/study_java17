package ch18;

// Exercise 18.13: SomeClass.java
public class SomeClass {

    public static String someMethod(int[] array2, int x) {
        if (x < array2.length) {
            return String.format("%s%d ", someMethod(array2, x + 1), array2[x]);
        } else {
            return "";
        }
    }

    public static void main(String[] args) {
        int[] array = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        String results = someMethod(array, 0);
        System.out.println(results);
    }
/*
10 9 8 7 6 5 4 3 2 1
 */
}
