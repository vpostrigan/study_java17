package baeldung_com;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * https://www.baeldung.com/java-caching-caffeine
 */
class JavaCachingCaffeineTest {

    // 3 strategies for cache population

    @Test
    public void test11() throws InterruptedException {
        // Populating Cache
        // 1. Manual Populating (manually put values into the cache and retrieve them later)
        Cache<String, DataObject> cache = Caffeine.newBuilder()
                .expireAfterAccess(8, TimeUnit.SECONDS)
                .maximumSize(10)
                .build();
        for (int i = 0; i < 15; i++) {
            cache.put("" + i, DataObject.of("" + i)); // populate the cache manually
            Thread.sleep(250);
            System.out.println(cache.asMap());
        }

        // get some value from the cache using the getIfPresent method.
        // This method will return null if the value is not present in the cache
        Assertions.assertNotNull(cache.getIfPresent("7"));
        cache.invalidate("7");
        Assertions.assertNull(cache.getIfPresent("7"));
        //  get method with Function that will fill cache if absent
        Assertions.assertNotNull(cache.get("7", k -> DataObject.of("7")));
        // Function will be called only once, even if several threads ask for the value simultaneously
        // That's why using get is preferable to getIfPresent.

        Map<String, DataObject> dataObjectMap
                = cache.getAllPresent(Arrays.asList("7", "150", "5"));
        System.out.println(dataObjectMap);
        Assertions.assertEquals(2, dataObjectMap.size());

        Assertions.assertNotNull(cache.getIfPresent("11"));
        System.out.println("stop");
        for (int i = 0; i < 100; i++) {
            cache.getIfPresent("11");
            Thread.sleep(250);
            System.out.println(cache.asMap());

            // The get method performs the computation atomically.
            // This means that the computation will be made only once —
            // even if several threads ask for the value simultaneously.
            // That's why using get is preferable to getIfPresent.
            cache.get("10000", k -> DataObject.of("Data for A"));
        }
        System.out.println("stop2");
        for (int i = 0; i < 100; i++) {
            Thread.sleep(1000);
            System.out.println(cache.asMap());
        }
    }

    @Test
    public void test12() throws InterruptedException {
        // Populating Cache
        // 2. Synchronous Loading
        LoadingCache<String, DataObject> cache = Caffeine.newBuilder()
                .expireAfterWrite(8, TimeUnit.SECONDS)
                .maximumSize(10)
                .build(k -> { // get or getAll will use this function
                    System.out.println("> CALL: " + "Data for " + k);
                    return DataObject.of("Data for " + k);
                });

        String key = "10";
        DataObject dataObject = cache.get(key);
        for (int i = 0; i < 20; i++) {
            DataObject dataObject2 = cache.get(key);
            System.out.println(dataObject2);
            Thread.sleep(1000);
        }

        Assertions.assertNotNull(dataObject);
        Assertions.assertEquals("Data for " + key, dataObject.getData());

        Map<String, DataObject> dataObjectMap = cache.getAll(Arrays.asList("A", "B", "C"));
        Assertions.assertEquals(3, dataObjectMap.size());
    }

    @Test
    public void test13() {
        // Populating Cache
        // 3. Asynchronous Loading
        // (This strategy works the same as the previous but performs operations
        // asynchronously and returns a CompletableFuture holding the actual value)
        AsyncLoadingCache<String, DataObject> cache = Caffeine.newBuilder()
                .maximumSize(100)
                .expireAfterWrite(1, TimeUnit.MINUTES)
                .buildAsync(k -> { // get or getAll will use this function
                    System.out.println("> CALL: " + "Data for " + k);
                    return DataObject.of("Data for " + k);
                });

        String key = "A";

        cache.get(key)
                .thenAccept(dataObject -> {
                    Assertions.assertNotNull(dataObject);
                    Assertions.assertEquals("Data for " + key, dataObject.getData());
                });

        cache.getAll(Arrays.asList("A", "B", "C"))
                .thenAccept(dataObjectMap -> {
                    Assertions.assertEquals(3, dataObjectMap.size());
                });
    }

    // 3 strategies for value eviction: size-based, time-based, and reference-based.

    @Test
    public void test21() {
        // 1. Size-Based Eviction
        // When the cache is initialized, its size is equal to zero:
        LoadingCache<String, DataObject> cache = Caffeine.newBuilder()
                .maximumSize(1)
                .build(k -> DataObject.of("Data for " + k));

        Assertions.assertEquals(0, cache.estimatedSize());

        cache.get("A");
        Assertions.assertEquals(1, cache.estimatedSize());

        cache.get("B");
        // await the completion of the eviction
        // (cache eviction is executed asynchronously, and this method helps to await the completion of the eviction)
        cache.cleanUp();
        Assertions.assertEquals(1, cache.estimatedSize());

        // weigher Function to get the size of the cache
        cache = Caffeine.newBuilder()
                .maximumWeight(10)
                .weigher((k, v) -> {
                    int n = 5;
                    System.out.println("weigher: " + n + "   k: " + k + "   v: " + v);
                    return n;
                })
                .build(k -> {
                    System.out.println("Data for " + k);
                    return DataObject.of("Data for " + k);
                });

        Assertions.assertEquals(0, cache.estimatedSize());

        cache.get("A");
        Assertions.assertEquals(1, cache.estimatedSize());

        cache.get("B");
        Assertions.assertEquals(2, cache.estimatedSize());

        // values are removed from the cache when the weight is over 10
        cache.get("C");
        cache.cleanUp();

        Assertions.assertEquals(2, cache.estimatedSize());
    }

    @Test
    public void test22() {
        // 2. Time-Based Eviction

        // Expire after access — entry is expired after period is passed since the last read or write occurs
        LoadingCache<String, DataObject> cache = Caffeine.newBuilder()
                .expireAfterAccess(5, TimeUnit.MINUTES)
                .build(k -> DataObject.of("Data for " + k));

        // Expire after write — entry is expired after period is passed since the last write occurs
        cache = Caffeine.newBuilder()
                .expireAfterWrite(10, TimeUnit.SECONDS)
                .weakKeys()
                .weakValues()
                .build(k -> DataObject.of("Data for " + k));

        // Custom policy — an expiration time is calculated for each entry individually by the Expiry implementation
        cache = Caffeine.newBuilder().expireAfter(new Expiry<String, DataObject>() {
            @Override
            public long expireAfterCreate(String key, DataObject value, long currentTime) {
                return value.getData().length() * 1000;
            }

            @Override
            public long expireAfterUpdate(String key, DataObject value, long currentTime, long currentDuration) {
                return currentDuration;
            }

            @Override
            public long expireAfterRead(String key, DataObject value, long currentTime, long currentDuration) {
                return currentDuration;
            }
        }).build(k -> DataObject.of("Data for " + k));
    }

    @Test
    public void test23() {
        // 3. Reference-Based Eviction

        // WeakRefence usage allows garbage-collection of objects when there are not any strong references to the object
        LoadingCache<String, DataObject> cache = Caffeine.newBuilder()
                .expireAfterWrite(10, TimeUnit.SECONDS)
                .weakKeys()
                .weakValues()
                .build(k -> DataObject.of("Data for " + k));

        // SoftReference allows objects to be garbage-collected based on the global Least-Recently-Used strategy of the JVM
        cache = Caffeine.newBuilder()
                .expireAfterWrite(10, TimeUnit.SECONDS)
                .softValues()
                .build(k -> DataObject.of("Data for " + k));
    }

    // //

    @Test
    public void test31() {
        // Refreshing
        Caffeine.newBuilder()
                .refreshAfterWrite(1, TimeUnit.MINUTES)
                .build(k -> DataObject.of("Data for " + k));

        // expireAfter
        // When the expired entry is requested, an execution blocks until the new value
        // would have been calculated by the build Function

        // refreshAfter
        // if the entry is eligible for the refreshing, then the cache
        // would return an old value and asynchronously reload the value.
    }

    // //

    @Test
    public void test41() {
        LoadingCache<String, DataObject> cache = Caffeine.newBuilder()
                .maximumSize(100)
                .recordStats()
                .build(k -> DataObject.of("Data for " + k));
        cache.get("A");
        cache.get("A");

        Assertions.assertEquals(1, cache.stats().hitCount());
        Assertions.assertEquals(1, cache.stats().missCount());
    }

    private static class DataObject {
        private final String data;

        private static int objectCounter = 0;

        private DataObject(String data) {
            this.data = data;
        }

        public String getData() {
            return data;
        }

        @Override
        public String toString() {
            return "data='" + data + '\'';
        }

        public static DataObject of(String data) {
            objectCounter++;
            return new DataObject(data);
        }
    }

}
