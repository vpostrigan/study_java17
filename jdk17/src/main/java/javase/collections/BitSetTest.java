package javase.collections;

import java.util.BitSet;

public class BitSetTest {

    public static void main(String args[]) {
        BitSet bits1 = new BitSet(16);
        BitSet bits2 = new BitSet(16);

        // set some bits
        for (int i = 0; i < 16; i++) {
            if ((i % 2) == 0) bits1.set(i);
            if ((i % 5) != 0) bits2.set(i);
        }

        // {0, 2, 4, 6, 8, 10, 12, 14}
        System.out.println("Исходная закономерность в bits1: " + bits1);
        // {1, 2, 3, 4, 6, 7, 8, 9, 11, 12, 13, 14}
        System.out.println("Исходная закономерность в bits2: " + bits2);
        System.out.println();

        // AND биты
        bits2.and(bits1);
        System.out.println("bits2 AND bits1: "); // {2, 4, 6, 8, 12, 14}
        System.out.println(bits2);

        // OR биты
        bits2.or(bits1);
        System.out.println("bits2 OR bits1: "); // {0, 2, 4, 6, 8, 10, 12, 14}
        System.out.println(bits2);

        // XOR биты
        bits2.xor(bits1);
        System.out.println("bits2 XOR bits1: "); // {}
        System.out.println(bits2);
    }

}
