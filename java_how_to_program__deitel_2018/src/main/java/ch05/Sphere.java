package ch05;

import java.util.Scanner;

// Exercise 5.6: Sphere.java
// Calculate the volume of a sphere.
public class Sphere {

    // obtain radius from user and display volume of sphere
    public static void main(String[] args) {
        Scanner input = new Scanner(System.in);

        System.out.print("Enter radius of sphere: ");
        double radius = input.nextDouble();

        System.out.printf("Volume is %f%n", sphereVolume(radius));
    }

    // calculate and return sphere volume
    public static double sphereVolume(double radius) {
        double volume = (4.0 / 3.0) * Math.PI * Math.pow(radius, 3);
        return volume;
    }
/*
Enter radius of sphere: 10.5
Volume is 4849.048261
 */
}
