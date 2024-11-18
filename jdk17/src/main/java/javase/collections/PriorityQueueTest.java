package javase.collections;

import java.util.PriorityQueue;
import java.util.Queue;

public class PriorityQueueTest {

    public static void main(String[] args) {
        Queue<String> q = new PriorityQueue<>(/*String.CASE_INSENSITIVE_ORDER*/);
        q.add("4");
        q.add("3");
        q.add("2");
        q.add("1");
        q.add("0");
        q.add("00");
        q.add("100");

        System.out.println(q); // [0, 1, 00, 4, 2, 3, 100] (shows unordered?)
        System.out.println(q.remove()); // 0
        System.out.println(q.remove()); // 00
        System.out.println(q.remove()); // 1
        System.out.println(q.remove()); // 100
        System.out.println(q.remove()); // 2
        System.out.println(q.remove()); // 3
        System.out.println(q.remove()); // 4
    }

}