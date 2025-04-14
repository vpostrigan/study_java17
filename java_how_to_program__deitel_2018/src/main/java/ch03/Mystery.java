package ch03;

// Exercise 3.16: Mystery.java
public class Mystery {

    public static void main(String[] args) {
        int x = 1;
        int total = 0;

        while (x <= 10) {
            int y = x * x;
            System.out.println(y);
            total += y;
            ++x;
        }

        System.out.printf("Total is %d%n", total);
    }
/*
1
4
9
16
25
36
49
64
81
100
Total is 385
 */
}
