package javase.collections;

import javase.collections.BinarySearcher.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static javase.collections.BinarySearcher.findMidpoint;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BinarySearchTest {
    private BinarySearcher searcher;

    @Test
    public void createSearcher() throws Exception {
        searcher = new BinarySearcher(new long[1]);
    }

    @Test
    public void nullInputThrowsException() throws Exception {
        Assertions.assertThrows(InvalidArray.class, () -> {
            searcher = new BinarySearcher(null);
        });
    }

    @Test
    public void zeroSizedArrayFindsNothing() throws Exception {
        searcher = new BinarySearcher(new long[0]);
        assertEquals(0, searcher.findLowerBound(1));
    }

    @Test
    public void checkInOrder() throws Exception {
        long[][] inOrderArrays = {{0}, {0, 1}, {0, 1, 2, 3}, {0, 1, 1, 2, 3}};
        for (long[] inOrder : inOrderArrays) {
            searcher = new BinarySearcher(inOrder);
            searcher.validate();
        }
    }

    @Test
    public void outOfOrderThrowsException() throws Exception {
        long[][] outOfOrderArrays = {{1, 0}, {1, 0, 2}, {0, 2, 1}, {0, 1, 2, 4, 3}};
        int exceptions = 0;
        for (long[] outOfOrder : outOfOrderArrays) {
            searcher = new BinarySearcher(outOfOrder);
            try {
                searcher.validate();
            } catch (OutOfOrderArray e) {
                exceptions++;
            }
        }
        assertEquals(outOfOrderArrays.length, exceptions);
    }

    @Test
    public void findsProperMidpoint() throws Exception {
        assertEquals(0, findMidpoint(0, 0));
        assertEquals(0, findMidpoint(0, 1));
        assertEquals(1, findMidpoint(0, 2));
        assertEquals(1, findMidpoint(0, 3));
        assertEquals(Integer.MAX_VALUE / 2, findMidpoint(0, Integer.MAX_VALUE));
        assertEquals(Integer.MAX_VALUE / 2 + 1, findMidpoint(1, Integer.MAX_VALUE));
        assertEquals(Integer.MAX_VALUE / 2 + 1, findMidpoint(2, Integer.MAX_VALUE));
        assertEquals(Integer.MAX_VALUE / 2 + 2, findMidpoint(3, Integer.MAX_VALUE));
        assertEquals(Integer.MAX_VALUE / 2, findMidpoint(1, Integer.MAX_VALUE - 2));
    }

    private void assertFound(int target, int index, long[] domain) {
        BinarySearcher searcher = new BinarySearcher(domain);
        assertTrue(searcher.find(target));
        assertEquals(index, searcher.findLowerBound(target));
    }

    private void assertNotFound(int target, int index, long[] domain) {
        BinarySearcher searcher = new BinarySearcher(domain);
        assertFalse(searcher.find(target));
        assertEquals(index, searcher.findLowerBound(target));
    }

    @Test
    public void simpleFinds() throws Exception {
        assertFound(0, 0, new long[]{0});
        assertFound(5, 2, new long[]{0, 1, 5, 7});
        assertFound(7, 3, new long[]{0, 1, 5, 7});
        assertFound(7, 5, new long[]{0, 1, 2, 2, 3, 7, 8});
        assertFound(2, 2, new long[]{0, 1, 2, 2, 3, 7, 8});
        assertFound(2, 2, new long[]{0, 1, 2, 2, 2, 3, 7, 8});

        assertNotFound(1, 1, new long[]{0});
        assertNotFound(6, 3, new long[]{1, 2, 5, 7, 9});
        assertNotFound(0, 0, new long[]{1, 2, 2, 5, 7, 9});
        assertNotFound(10, 6, new long[]{1, 2, 2, 5, 7, 9});
    }

    long[] makeArray(int n) {
        long[] array = new long[n];
        for (int i = 0; i < n; i++)
            array[i] = i;
        return array;
    }

    private void assertCompares(int compares, int n) {
        long[] array = makeArray(n);
        BinarySearcherSpy spy = new BinarySearcherSpy(array);
        spy.findLowerBound(0);
        assertTrue(spy.compares > 0);
        assertTrue(spy.compares <= compares + 2, "" + spy.compares);
    }

    @Test
    public void logNCheck() throws Exception {
        assertCompares(1, 1);
        assertCompares(5, 32);
        assertCompares(16, 65536);
    }
}

class BinarySearcherSpy extends BinarySearcher {
    public int compares = 0;

    public BinarySearcherSpy(long[] array) {
        super(array);
    }

    protected int findLowerBound(int l, int r, int element) {
        compares++;
        return super.findLowerBound(l, r, element);
    }
}
