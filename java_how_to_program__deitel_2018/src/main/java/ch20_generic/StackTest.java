package ch20_generic;

import java.util.NoSuchElementException;

// Fig. 20.8: StackTest.java
// Stack generic class test program.
public class StackTest {

    public static void main(String[] args) {
        double[] doubleElements = {1.1, 2.2, 3.3, 4.4, 5.5};
        int[] integerElements = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

        // Create a Stack<Double> and a Stack<Integer>
        Stack<Double> doubleStack = new Stack<>(5);
        Stack<Integer> integerStack = new Stack<>();

        // push elements of doubleElements onto doubleStack
        testPushDouble(doubleStack, doubleElements);
        testPopDouble(doubleStack); // pop from doubleStack

        // push elements of integerElements onto integerStack
        testPushInteger(integerStack, integerElements);
        testPopInteger(integerStack); // pop from integerStack
    }

    // test push method with double stack
    private static void testPushDouble(Stack<Double> stack, double[] values) {
        System.out.printf("%nPushing elements onto doubleStack%n");

        // push elements to Stack
        for (double value : values) {
            System.out.printf("%.1f ", value);
            stack.push(value); // push onto doubleStack
        }
    }

    // test pop method with double stack
    private static void testPopDouble(Stack<Double> stack) {
        // pop elements from stack
        try {
            System.out.printf("%nPopping elements from doubleStack%n");
            double popValue; // store element removed from stack

            // remove all elements from Stack
            while (true) {
                popValue = stack.pop(); // pop from doubleStack
                System.out.printf("%.1f ", popValue);
            }
        } catch (NoSuchElementException noSuchElementException) {
            System.err.println();
            noSuchElementException.printStackTrace();
        }
    }

    // test push method with integer stack
    private static void testPushInteger(Stack<Integer> stack, int[] values) {
        System.out.printf("%nPushing elements onto integerStack%n");

        // push elements to Stack
        for (int value : values) {
            System.out.printf("%d ", value);
            stack.push(value); // push onto integerStack
        }
    }

    // test pop method with integer stack
    private static void testPopInteger(Stack<Integer> stack) {
        // pop elements from stack
        try {
            System.out.printf("%nPopping elements from integerStack%n");
            int popValue; // store element removed from stack

            // remove all elements from Stack
            while (true) {
                popValue = stack.pop(); // pop from intStack
                System.out.printf("%d ", popValue);
            }
        } catch (NoSuchElementException noSuchElementException) {
            System.err.println();
            noSuchElementException.printStackTrace();
        }
    }
/*
Pushing elements onto doubleStack
1.1 2.2 3.3 4.4 5.5
Popping elements from doubleStack
5.5 4.4 3.3 2.2 1.1

Pushing elements onto integerStack
1 2 3 4 5 6 7 8 9 10
Popping elements from integerStack
10 9 8 7 6 5 4 3 2 1
java.util.NoSuchElementException: Stack is empty, cannot pop
	at ch20_generic.Stack.pop(Stack.java:30)
	at ch20_generic.StackTest.testPopDouble(StackTest.java:46)
	at ch20_generic.StackTest.main(StackTest.java:19)

java.util.NoSuchElementException: Stack is empty, cannot pop
	at ch20_generic.Stack.pop(Stack.java:30)
	at ch20_generic.StackTest.testPopInteger(StackTest.java:75)
	at ch20_generic.StackTest.main(StackTest.java:23)
 */
}
