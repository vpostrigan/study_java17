package ch05;

// Fig. 5.10: MethodOverload.java
// Overloaded method declarations.
public class MethodOverload {

    // test overloaded square methods
    public static void main(String[] args) {
        System.out.printf("Square of integer 7 is %d%n", square(7));
        System.out.printf("Square of double 7.5 is %f%n", square(7.5));
    }

    // square method with int argument
    public static int square(int intValue) {
        System.out.printf("%nCalled square with int argument: %d%n", intValue);
        return intValue * intValue;
    }

    // square method with double argument
    public static double square(double doubleValue) {
        System.out.printf("%nCalled square with double argument: %f%n", doubleValue);
        return doubleValue * doubleValue;
    }
/*
Called square with int argument: 7
Square of integer 7 is 49

Called square with double argument: 7.500000
Square of double 7.5 is 56.250000
 */
}
