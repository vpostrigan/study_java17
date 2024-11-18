package javase.collections;

/**
 * Java code to implement binary search on Singly Linked List
 */
public class LinkedList_BinarySearch {

    static Node pushFirst(Node head, int data) {
        Node newNode = new Node(data);
        newNode.next = head;
        head = newNode;
        return head;
    }

    // Function to find middle element using Fast and Slow pointers
    static Node middleNode(Node start, Node last) {
        if (start == null)
            return null;

        Node slow = start;
        Node fast = start.next;

        while (fast != last) {
            fast = fast.next;
            if (fast != last) {
                slow = slow.next;
                fast = fast.next;
            }
        }
        return slow;
    }

    static Node binarySearch(Node head, int value) {
        Node start = head;
        Node last = null;

        do {
            // Find Middle
            Node mid = middleNode(start, last);

            if (mid == null)
                return null;

            if (mid.data == value) { // If value is present at middle
                return mid;
            } else if (mid.data > value) { // If value is less than mid
                start = mid.next;
            } else {
                last = mid; // If the value is more than mid.
            }
        } while (last == null || last != start);

        // value not present
        return null;
    }

    public static void main(String[] args) {
        Node head = null;

        head = pushFirst(head, 1);
        head = pushFirst(head, 4);
        head = pushFirst(head, 7);
        head = pushFirst(head, 8);
        head = pushFirst(head, 9);
        head = pushFirst(head, 10);
        // 10 , 9 , 8 , 7 , 4 , 1

        int value = 7;
        if (binarySearch(head, value) == null) {
            System.out.println("Value not present");
        } else {
            System.out.println("Present");
        }
    }

    static class Node {
        int data;
        Node next;

        Node(int d) {
            data = d;
            next = null;
        }
    }
}