package ch21;

import java.util.NoSuchElementException;

// Fig. 21.9: Stack.java
// Stack uses a composed List object.

public class Stack<E> {
    private List<E> stackList;

    // constructor
    public Stack() {
        stackList = new List<>("stack");
    }

    // add object to stack
    public void push(E object) {
        stackList.insertAtFront(object);
    }

    // remove object from stack
    public E pop() throws NoSuchElementException {
        return stackList.removeFromFront();
    }

    // determine if stack is empty
    public boolean isEmpty() {
        return stackList.isEmpty();
    }

    // output stack contents
    public void print() {
        stackList.print();
    }
}

// Fig. 21.10: StackTest.java
// Stack manipulation program.
class StackTest {

    public static void main(String[] args) {
        Stack<Integer> stack = new Stack<>();

        // use push method
        stack.push(-1);
        stack.print();
        stack.push(0);
        stack.print();
        stack.push(1);
        stack.print();
        stack.push(5);
        stack.print();

        // remove items from stack
        boolean continueLoop = true;

        while (continueLoop) {
            try {
                int removedItem = stack.pop(); // remove top element
                System.out.printf("%n%d popped%n", removedItem);
                stack.print();
            } catch (NoSuchElementException noSuchElementException) {
                continueLoop = false;
                noSuchElementException.printStackTrace();
            }
        }
    }
/*
The stack is: -1
The stack is: 0 -1
The stack is: 1 0 -1
The stack is: 5 1 0 -1

5 popped
The stack is: 1 0 -1

1 popped
The stack is: 0 -1

0 popped
The stack is: -1

-1 popped
Empty stack
 */
}
