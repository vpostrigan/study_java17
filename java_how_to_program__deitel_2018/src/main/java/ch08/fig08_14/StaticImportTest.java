package ch08.fig08_14;

import static java.lang.Math.*;

// Fig. 8.14: StaticImportTest.java
// Static import of Math class methods.
public class StaticImportTest {

    public static void main(String[] args) {
        System.out.printf("sqrt(900.0) = %.1f%n", sqrt(900.0));
        System.out.printf("ceil(-9.8) = %.1f%n", ceil(-9.8));
        System.out.printf("E = %f%n", E);
        System.out.printf("PI = %f%n", PI);
    }
/*
sqrt(900.0) = 30.0
ceil(-9.8) = -9.0
E = 2.718282
PI = 3.141593
 */
}