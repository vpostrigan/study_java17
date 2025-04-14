package ch21;

import java.util.NoSuchElementException;

// Fig. 21.11: Queue.java
// Queue uses class List.

public class Queue<E> {
    private List<E> queueList;

    // constructor
    public Queue() {
        queueList = new List<>("queue");
    }

    // add object to queue
    public void enqueue(E object) {
        queueList.insertAtBack(object);
    }

    // remove object from queue
    public E dequeue() throws NoSuchElementException {
        return queueList.removeFromFront();
    }

    // determine if queue is empty
    public boolean isEmpty() {
        return queueList.isEmpty();
    }

    // output queue contents
    public void print() {
        queueList.print();
    }
}

// Fig. 21.12: QueueTest.java
// Class QueueTest.
class QueueTest {

    public static void main(String[] args) {
        Queue<Integer> queue = new Queue<>();

        // use enqueue method
        queue.enqueue(-1);
        queue.print();
        queue.enqueue(0);
        queue.print();
        queue.enqueue(1);
        queue.print();
        queue.enqueue(5);
        queue.print();

        // remove objects from queue
        boolean continueLoop = true;

        while (continueLoop) {
            try {
                int removedItem = queue.dequeue(); // remove head element
                System.out.printf("%n%d dequeued%n", removedItem);
                queue.print();
            } catch (NoSuchElementException noSuchElementException) {
                continueLoop = false;
                noSuchElementException.printStackTrace();
            }
        }
    }
/*
The queue is: -1
The queue is: -1 0
The queue is: -1 0 1
The queue is: -1 0 1 5

-1 dequeued
The queue is: 0 1 5

0 dequeued
The queue is: 1 5

1 dequeued
The queue is: 5

5 dequeued
Empty queue
 */
}
