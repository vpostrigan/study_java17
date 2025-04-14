package ch05;

// Exercise 5.3: MathTest.java
// Testing the Math class methods.
public class MathTest {

    public static void main(String[] args) {
        System.out.printf("Math.abs(23.7) = %f%n", Math.abs(23.7)); // Math.abs(23.7) = 23.700000
        System.out.printf("Math.abs(0.0) = %f%n", Math.abs(0.0)); // Math.abs(0.0) = 0.000000
        System.out.printf("Math.abs(-23.7) = %f%n", Math.abs(-23.7)); // Math.abs(-23.7) = 23.700000
        System.out.printf("Math.ceil(9.2) = %f%n", Math.ceil(9.2)); // Math.ceil(9.2) = 10.000000
        System.out.printf("Math.ceil(-9.8) = %f%n", Math.ceil(-9.8)); // Math.ceil(-9.8) = -9.000000
        System.out.printf("Math.cos(0.0) = %f%n", Math.cos(0.0)); // Math.cos(0.0) = 1.000000
        System.out.printf("Math.exp(1.0) = %f%n", Math.exp(1.0)); // Math.exp(1.0) = 2.718282
        System.out.printf("Math.exp(2.0) = %f%n", Math.exp(2.0)); // Math.exp(2.0) = 7.389056
        System.out.printf("Math.floor(9.2) = %f%n", Math.floor(9.2)); // Math.floor(9.2) = 9.000000
        System.out.printf("Math.floor(-9.8) = %f%n", Math.floor(-9.8)); // Math.floor(-9.8) = -10.000000
        System.out.printf("Math.log(Math.E) = %f%n", Math.log(Math.E)); // Math.log(Math.E) = 1.000000
        System.out.printf("Math.log(Math.E * Math.E) = %f%n", Math.log(Math.E * Math.E)); // Math.log(Math.E * Math.E) = 2.000000
        System.out.printf("Math.max(2.3, 12.7) = %f%n", Math.max(2.3, 12.7)); // Math.max(2.3, 12.7) = 12.700000
        System.out.printf("Math.max(-2.3, -12.7) = %f%n", Math.max(-2.3, -12.7)); // Math.max(-2.3, -12.7) = -2.300000
        System.out.printf("Math.min(2.3, 12.7) = %f%n", Math.min(2.3, 12.7)); // Math.min(2.3, 12.7) = 2.300000
        System.out.printf("Math.min(-2.3, -12.7) = %f%n", Math.min(-2.3, -12.7)); // Math.min(-2.3, -12.7) = -12.700000
        System.out.printf("Math.pow(2.0, 7.0) = %f%n", Math.pow(2.0, 7.0)); // Math.pow(2.0, 7.0) = 128.000000
        System.out.printf("Math.pow(9.0, 0.5) = %f%n", Math.pow(9.0, 0.5)); // Math.pow(9.0, 0.5) = 3.000000
        System.out.printf("Math.sin(0.0) = %f%n", Math.sin(0.0)); // Math.sin(0.0) = 0.000000
        System.out.printf("Math.sqrt(900.0) = %f%n", Math.sqrt(900.0)); // Math.sqrt(900.0) = 30.000000
        System.out.printf("Math.tan(0.0) = %f%n", Math.tan(0.0)); // Math.tan(0.0) = 0.000000
    }

}
