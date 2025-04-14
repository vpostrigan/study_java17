package ch18;

// Fig. 18.3: FactorialCalculator.java
// Recursive factorial method.
public class FactorialCalculator03 {

    // recursive method factorial (assumes its parameter is >= 0)
    public static long factorial(long number) {
        if (number <= 1) { // test for base case
            return 1; // base cases: 0! = 1 and 1! = 1
        } else { // recursion step
            return number * factorial(number - 1);
        }
    }

    public static void main(String[] args) {
        // calculate the factorials of 0 through 21
        for (int counter = 0; counter <= 21; counter++) {
            System.out.printf("%d! = %d%n", counter, factorial(counter));
        }
    }
/*
0! = 1
1! = 1
2! = 2
3! = 6
4! = 24
5! = 120
6! = 720
7! = 5040
8! = 40320
9! = 362880
10! = 3628800
11! = 39916800
12! = 479001600
13! = 6227020800
14! = 87178291200
15! = 1307674368000
16! = 20922789888000
17! = 355687428096000
18! = 6402373705728000
19! = 121645100408832000
20! = 2432902008176640000
21! = -4249290049419214848
 */
}
