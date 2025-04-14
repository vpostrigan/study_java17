package ch20_generic;

import java.util.NoSuchElementException;

// Fig. 20.9: StackTest2.java
// Passing generic Stack objects to generic methods.
public class StackTest2 {

    public static void main(String[] args) {
        Double[] doubleElements = {1.1, 2.2, 3.3, 4.4, 5.5};
        Integer[] integerElements = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

        // Create a Stack<Double> and a Stack<Integer>
        Stack<Double> doubleStack = new Stack<>(5);
        Stack<Integer> integerStack = new Stack<>();

        // push elements of doubleElements onto doubleStack
        testPush("doubleStack", doubleStack, doubleElements);
        testPop("doubleStack", doubleStack); // pop from doubleStack

        // push elements of integerElements onto integerStack
        testPush("integerStack", integerStack, integerElements);
        testPop("integerStack", integerStack); // pop from integerStack
    }

    // generic method testPush pushes elements onto a Stack
    public static <E> void testPush(String name, Stack<E> stack, E[] elements) {
        System.out.printf("%nPushing elements onto %s%n", name);

        // push elements onto Stack
        for (E element : elements) {
            System.out.printf("%s ", element);
            stack.push(element); // push element onto stack
        }
    }

    // generic method testPop pops elements from a Stack
    public static <E> void testPop(String name, Stack<E> stack) {
        // pop elements from stack
        try {
            System.out.printf("%nPopping elements from %s%n", name);
            E popValue; // store element removed from stack

            // remove all elements from Stack
            while (true) {
                popValue = stack.pop();
                System.out.printf("%s ", popValue);
            }
        } catch (NoSuchElementException noSuchElementException) {
            System.out.println();
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
	at ch20_generic.StackTest2.testPop(StackTest2.java:47)
	at ch20_generic.StackTest2.main(StackTest2.java:20)
java.util.NoSuchElementException: Stack is empty, cannot pop
	at ch20_generic.Stack.pop(Stack.java:30)
	at ch20_generic.StackTest2.testPop(StackTest2.java:47)
	at ch20_generic.StackTest2.main(StackTest2.java:24)
 */
}
