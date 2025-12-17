package com.minileaf.core.storage.cached

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.RepeatedTest
import org.junit.jupiter.api.Timeout
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.concurrent.thread

/**
 * Comprehensive tests for LRUCache focusing on data corruption prevention.
 * These tests use high thread counts and iterations to expose race conditions.
 */
class LRUCacheTest {

    // ==================== Basic Functionality Tests ====================

    @Test
    fun `basic put and get`() {
        val cache = LRUCache<Int, String>(maxSize = 3)

        cache.put(1, "one")
        cache.put(2, "two")
        cache.put(3, "three")

        assertThat(cache.get(1)).isEqualTo("one")
        assertThat(cache.get(2)).isEqualTo("two")
        assertThat(cache.get(3)).isEqualTo("three")
        assertThat(cache.size()).isEqualTo(3)
    }

    @Test
    fun `evicts least recently used when at capacity`() {
        val evicted = mutableListOf<Pair<Int, String>>()
        val cache = LRUCache<Int, String>(
            maxSize = 3,
            onEvict = { k, v -> evicted.add(k to v) }
        )

        // Fill cache
        cache.put(1, "one")
        cache.put(2, "two")
        cache.put(3, "three")

        // Access 1 and 2 (making 3 the LRU)
        cache.get(1)
        cache.get(2)

        // Insert 4, should evict 3
        cache.put(4, "four")

        assertThat(cache.get(1)).isEqualTo("one")
        assertThat(cache.get(2)).isEqualTo("two")
        assertThat(cache.get(3)).isNull()
        assertThat(cache.get(4)).isEqualTo("four")
        assertThat(cache.size()).isEqualTo(3)
        assertThat(evicted).containsExactly(3 to "three")
    }

    @Test
    fun `update existing key does not change size`() {
        val cache = LRUCache<Int, String>(maxSize = 3)

        cache.put(1, "one")
        cache.put(2, "two")
        assertThat(cache.size()).isEqualTo(2)

        cache.put(1, "ONE")  // Update
        assertThat(cache.size()).isEqualTo(2)
        assertThat(cache.get(1)).isEqualTo("ONE")
    }

    @Test
    fun `remove works correctly`() {
        val cache = LRUCache<Int, String>(maxSize = 3)

        cache.put(1, "one")
        cache.put(2, "two")

        val removed = cache.remove(1)
        assertThat(removed).isEqualTo("one")
        assertThat(cache.get(1)).isNull()
        assertThat(cache.size()).isEqualTo(1)
    }

    @Test
    fun `clear removes all entries`() {
        val cache = LRUCache<Int, String>(maxSize = 3)

        cache.put(1, "one")
        cache.put(2, "two")
        cache.put(3, "three")

        cache.clear()

        assertThat(cache.size()).isEqualTo(0)
        assertThat(cache.get(1)).isNull()
        assertThat(cache.get(2)).isNull()
        assertThat(cache.get(3)).isNull()
    }

    // ==================== Concurrent Get Tests ====================

    @RepeatedTest(5)
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    fun `concurrent gets do not corrupt cache`() {
        val cache = LRUCache<Int, String>(maxSize = 100)

        // Pre-populate cache
        repeat(100) { i ->
            cache.put(i, "value-$i")
        }

        val errors = ConcurrentHashMap<Int, String>()
        val barrier = CyclicBarrier(50)

        val threads = (1..50).map { threadId ->
            thread {
                try {
                    barrier.await()  // Synchronize for maximum contention
                    repeat(1000) { iteration ->
                        val key = iteration % 100
                        val value = cache.get(key)

                        // Verify value is correct or null (after eviction)
                        if (value != null && value != "value-$key") {
                            errors[threadId] = "Thread $threadId iteration $iteration: " +
                                    "Expected 'value-$key' but got '$value'"
                        }
                    }
                } catch (e: Exception) {
                    errors[threadId] = "Thread $threadId failed: ${e.message}"
                }
            }
        }

        threads.forEach { it.join() }

        if (errors.isNotEmpty()) {
            errors.forEach { (thread, error) ->
                println("ERROR in thread $thread: $error")
            }
        }
        assertThat(errors).isEmpty()
    }

    @RepeatedTest(5)
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    fun `concurrent gets on same key do not corrupt`() {
        val cache = LRUCache<String, String>(maxSize = 10)
        cache.put("key", "value")

        val errors = ConcurrentHashMap<Int, String>()
        val barrier = CyclicBarrier(100)

        val threads = (1..100).map { threadId ->
            thread {
                try {
                    barrier.await()
                    repeat(1000) {
                        val value = cache.get("key")
                        if (value != null && value != "value") {
                            errors[threadId] = "Got wrong value: $value"
                        }
                    }
                } catch (e: Exception) {
                    errors[threadId] = "Exception: ${e.message}"
                }
            }
        }

        threads.forEach { it.join() }
        assertThat(errors).isEmpty()
    }

    // ==================== Concurrent Put Tests ====================

    @RepeatedTest(5)
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    fun `concurrent puts do not lose updates`() {
        val cache = LRUCache<Int, AtomicInteger>(maxSize = 1000)

        val barrier = CyclicBarrier(50)
        val threads = (1..50).map { threadId ->
            thread {
                barrier.await()
                repeat(100) { i ->
                    val key = i
                    cache.put(key, AtomicInteger(threadId))
                }
            }
        }

        threads.forEach { it.join() }

        // Size should not exceed maxSize
        assertThat(cache.size()).isLessThanOrEqualTo(1000)

        // All values should be valid thread IDs
        for (i in 0 until cache.size()) {
            val value = cache.get(i)
            if (value != null) {
                assertThat(value.get()).isBetween(1, 50)
            }
        }
    }

    @RepeatedTest(5)
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    fun `concurrent puts on same key maintain consistency`() {
        val cache = LRUCache<String, String>(maxSize = 10)

        val barrier = CyclicBarrier(50)
        val threads = (1..50).map { threadId ->
            thread {
                barrier.await()
                repeat(100) {
                    cache.put("shared-key", "thread-$threadId")
                }
            }
        }

        threads.forEach { it.join() }

        // Cache should have exactly 1 entry
        assertThat(cache.size()).isEqualTo(1)

        // Value should be one of the thread values
        val value = cache.get("shared-key")
        assertThat(value).matches("thread-\\d+")
    }

    // ==================== Mixed Get/Put Tests ====================

    @RepeatedTest(5)
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    fun `concurrent mixed operations maintain consistency`() {
        val cache = LRUCache<Int, String>(maxSize = 100)

        // Pre-populate
        repeat(50) { i ->
            cache.put(i, "initial-$i")
        }

        val errors = ConcurrentHashMap<Int, String>()
        val barrier = CyclicBarrier(60)

        // 30 readers
        val readers = (1..30).map { threadId ->
            thread {
                try {
                    barrier.await()
                    repeat(1000) {
                        val key = (0..99).random()
                        val value = cache.get(key)

                        // If value exists, it should have correct format
                        // Format: "initial-{num}" or "updated-{threadId}-{iteration}"
                        if (value != null && !value.matches(Regex("(initial-\\d+|updated-\\d+-\\d+)"))) {
                            errors[threadId] = "Invalid value format: $value"
                        }
                    }
                } catch (e: Exception) {
                    errors[threadId] = "Reader $threadId: ${e.message}"
                }
            }
        }

        // 30 writers
        val writers = (31..60).map { threadId ->
            thread {
                try {
                    barrier.await()
                    repeat(500) { i ->
                        val key = (0..99).random()
                        cache.put(key, "updated-$threadId-$i")
                    }
                } catch (e: Exception) {
                    errors[threadId] = "Writer $threadId: ${e.message}"
                }
            }
        }

        (readers + writers).forEach { it.join() }

        if (errors.isNotEmpty()) {
            errors.forEach { (thread, error) ->
                println("ERROR in thread $thread: $error")
            }
        }
        assertThat(errors).isEmpty()
        assertThat(cache.size()).isLessThanOrEqualTo(100)
    }

    // ==================== Eviction Tests ====================

    @RepeatedTest(5)
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    fun `concurrent operations with evictions maintain consistency`() {
        val evictedCount = AtomicInteger(0)
        val cache = LRUCache<Int, String>(
            maxSize = 50,
            onEvict = { _, _ -> evictedCount.incrementAndGet() }
        )

        val errors = ConcurrentHashMap<Int, String>()
        val barrier = CyclicBarrier(40)

        val threads = (1..40).map { threadId ->
            thread {
                try {
                    barrier.await()
                    repeat(500) { i ->
                        // Mix of operations to cause evictions
                        val key = i % 100  // More keys than cache size
                        cache.put(key, "value-$threadId-$i")
                        cache.get(key % 50)
                    }
                } catch (e: Exception) {
                    errors[threadId] = "Thread $threadId: ${e.message}"
                }
            }
        }

        threads.forEach { it.join() }

        assertThat(errors).isEmpty()
        assertThat(cache.size()).isLessThanOrEqualTo(50)
        assertThat(evictedCount.get()).isGreaterThan(0)  // Some evictions should have happened
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    fun `eviction callback is always called correctly`() {
        val evicted = ConcurrentHashMap.newKeySet<Int>()
        val cache = LRUCache<Int, String>(
            maxSize = 10,
            onEvict = { k, _ -> evicted.add(k) }
        )

        // Insert 100 items (should evict 90)
        repeat(100) { i ->
            cache.put(i, "value-$i")
        }

        // Should have evicted exactly 90 items
        assertThat(evicted.size).isEqualTo(90)
        assertThat(cache.size()).isEqualTo(10)

        // Last 10 items should be in cache
        for (i in 90..99) {
            assertThat(cache.get(i)).isEqualTo("value-$i")
        }
    }

    @RepeatedTest(5)
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    fun `concurrent evictions do not cause corruption`() {
        val evicted = ConcurrentHashMap.newKeySet<Int>()
        val cache = LRUCache<Int, String>(
            maxSize = 20,
            onEvict = { k, _ -> evicted.add(k) }
        )

        val barrier = CyclicBarrier(30)
        val threads = (1..30).map { threadId ->
            thread {
                barrier.await()
                repeat(200) { i ->
                    val key = threadId * 1000 + i
                    cache.put(key, "value-$key")
                }
            }
        }

        threads.forEach { it.join() }

        // Cache should be at max capacity
        assertThat(cache.size()).isEqualTo(20)

        // Total inserted = 30 * 200 = 6000
        // Cache size = 20
        // Evicted should be = 6000 - 20 = 5980
        assertThat(evicted.size).isEqualTo(5980)

        // Evicted + remaining = total inserted
        assertThat(evicted.size + cache.size()).isEqualTo(6000)
    }

    // ==================== Remove Tests ====================

    @RepeatedTest(5)
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    fun `concurrent removes maintain consistency`() {
        val cache = LRUCache<Int, String>(maxSize = 100)

        // Pre-populate
        repeat(100) { i ->
            cache.put(i, "value-$i")
        }

        val barrier = CyclicBarrier(30)
        val removed = ConcurrentHashMap<Int, String>()

        val threads = (1..30).map { threadId ->
            thread {
                barrier.await()
                repeat(10) { i ->
                    val key = threadId * 10 + i
                    if (key < 100) {
                        val value = cache.remove(key)
                        if (value != null) {
                            removed[key] = value
                        }
                    }
                }
            }
        }

        threads.forEach { it.join() }

        // All removed keys should not be in cache
        removed.keys.forEach { key ->
            assertThat(cache.get(key)).isNull()
        }

        // Size should be consistent
        assertThat(cache.size()).isEqualTo(100 - removed.size)
    }

    // ==================== Mixed Operations Stress Test ====================

    @RepeatedTest(3)
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    fun `heavy mixed operations stress test`() {
        val cache = LRUCache<Int, String>(maxSize = 100)
        val errors = ConcurrentHashMap<Int, String>()
        val evictedCount = AtomicLong(0)
        val barrier = CyclicBarrier(100)

        // Rebuild cache with eviction callback
        val stressCache = LRUCache<Int, String>(
            maxSize = 100,
            onEvict = { _, _ -> evictedCount.incrementAndGet() }
        )

        val threads = (1..100).map { threadId ->
            thread {
                try {
                    barrier.await()

                    repeat(1000) { iteration ->
                        val key = iteration % 200  // More keys than cache size

                        when (iteration % 5) {
                            0 -> {
                                // Put
                                stressCache.put(key, "value-$threadId-$iteration")
                            }
                            1, 2 -> {
                                // Get (more frequent)
                                val value = stressCache.get(key)
                                if (value != null && !value.matches(Regex("value-\\d+-\\d+"))) {
                                    errors[threadId] = "Invalid value: $value"
                                }
                            }
                            3 -> {
                                // Remove
                                stressCache.remove(key)
                            }
                            4 -> {
                                // Check contains
                                stressCache.contains(key)
                            }
                        }
                    }
                } catch (e: Exception) {
                    errors[threadId] = "Thread $threadId failed: ${e.message}\n${e.stackTraceToString()}"
                }
            }
        }

        threads.forEach { it.join() }

        // Verify invariants
        assertThat(errors).isEmpty()
        assertThat(stressCache.size()).isLessThanOrEqualTo(100)
        assertThat(stressCache.size()).isGreaterThanOrEqualTo(0)

        println("Stress test completed: cache size=${stressCache.size()}, evictions=${evictedCount.get()}")
    }

    // ==================== LRU Order Correctness Tests ====================

    @Test
    fun `LRU order is maintained correctly with gets`() {
        val evicted = mutableListOf<Int>()
        val cache = LRUCache<Int, String>(
            maxSize = 3,
            onEvict = { k, _ -> evicted.add(k) }
        )

        // Insert 1, 2, 3
        cache.put(1, "one")
        cache.put(2, "two")
        cache.put(3, "three")

        // Access order: 1 (LRU) -> 2 -> 3 (MRU)

        // Access 1, making it MRU
        cache.get(1)
        // Order: 2 (LRU) -> 3 -> 1 (MRU)

        // Insert 4, should evict 2
        cache.put(4, "four")

        assertThat(evicted).containsExactly(2)
        assertThat(cache.get(1)).isNotNull()
        assertThat(cache.get(3)).isNotNull()
        assertThat(cache.get(4)).isNotNull()
    }

    @Test
    fun `LRU order is maintained correctly with updates`() {
        val evicted = mutableListOf<Int>()
        val cache = LRUCache<Int, String>(
            maxSize = 3,
            onEvict = { k, _ -> evicted.add(k) }
        )

        cache.put(1, "one")
        cache.put(2, "two")
        cache.put(3, "three")
        // Order: 1 (LRU) -> 2 -> 3 (MRU)

        // Update 1, making it MRU
        cache.put(1, "ONE")
        // Order: 2 (LRU) -> 3 -> 1 (MRU)

        // Insert 4, should evict 2
        cache.put(4, "four")

        assertThat(evicted).containsExactly(2)
        assertThat(cache.get(1)).isEqualTo("ONE")
        assertThat(cache.get(3)).isNotNull()
        assertThat(cache.get(4)).isNotNull()
    }

    // ==================== Size Consistency Tests ====================

    @RepeatedTest(5)
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    fun `size remains consistent under concurrent operations`() {
        val cache = LRUCache<Int, String>(maxSize = 50)

        val barrier = CyclicBarrier(40)
        val threads = (1..40).map { threadId ->
            thread {
                barrier.await()
                repeat(500) { i ->
                    when (i % 3) {
                        0 -> cache.put((threadId * 100 + i) % 100, "value")
                        1 -> cache.get((threadId * 100 + i) % 100)
                        2 -> cache.remove((threadId * 100 + i) % 100)
                    }

                    // Check size invariant continuously
                    val size = cache.size()
                    if (size > 50) {
                        throw AssertionError("Size exceeded max: $size")
                    }
                    if (size < 0) {
                        throw AssertionError("Size is negative: $size")
                    }
                }
            }
        }

        threads.forEach { it.join() }

        val finalSize = cache.size()
        assertThat(finalSize).isBetween(0, 50)
    }

    // ==================== Clear Tests ====================

    @RepeatedTest(5)
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    fun `clear during concurrent operations is safe`() {
        val cache = LRUCache<Int, String>(maxSize = 100)

        val barrier = CyclicBarrier(41)
        val errors = ConcurrentHashMap<Int, String>()

        // 1 thread clears repeatedly
        val clearThread = thread {
            barrier.await()
            repeat(50) {
                Thread.sleep(10)
                cache.clear()
            }
        }

        // 40 threads do mixed operations
        val workerThreads = (1..40).map { threadId ->
            thread {
                try {
                    barrier.await()
                    repeat(1000) { i ->
                        cache.put(i % 100, "value-$i")
                        cache.get(i % 100)
                    }
                } catch (e: Exception) {
                    errors[threadId] = "Thread $threadId: ${e.message}"
                }
            }
        }

        (workerThreads + clearThread).forEach { it.join() }

        assertThat(errors).isEmpty()
        // Cache should be valid (size between 0 and maxSize)
        assertThat(cache.size()).isBetween(0, 100)
    }

    // ==================== Data Integrity Tests ====================

    @RepeatedTest(5)
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    fun `no data corruption - values match keys`() {
        val cache = LRUCache<Int, String>(maxSize = 100)

        val barrier = CyclicBarrier(50)
        val errors = ConcurrentHashMap<Int, String>()

        val threads = (1..50).map { threadId ->
            thread {
                try {
                    barrier.await()
                    repeat(1000) { i ->
                        val key = i % 100

                        // Always store key in value for verification
                        cache.put(key, "key=$key")

                        // Verify immediately
                        val value = cache.get(key)
                        if (value != null && value != "key=$key") {
                            errors[threadId] = "Data corruption! Key=$key, Value=$value"
                        }
                    }
                } catch (e: Exception) {
                    errors[threadId] = "Exception in thread $threadId: ${e.message}"
                }
            }
        }

        threads.forEach { it.join() }

        // No errors should have occurred
        if (errors.isNotEmpty()) {
            errors.forEach { (thread, error) ->
                println("ERROR: $error")
            }
        }
        assertThat(errors).isEmpty()

        // Final verification: all values in cache should match their keys
        for (i in 0 until 100) {
            val value = cache.get(i)
            if (value != null) {
                assertThat(value).isEqualTo("key=$i")
            }
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    fun `no lost updates on same key`() {
        val cache = LRUCache<String, AtomicInteger>(maxSize = 10)
        cache.put("counter", AtomicInteger(0))

        val barrier = CyclicBarrier(50)
        val expectedSum = AtomicInteger(0)

        val threads = (1..50).map { threadId ->
            thread {
                barrier.await()
                repeat(100) {
                    // Read-modify-write
                    val counter = cache.get("counter")
                    if (counter != null) {
                        counter.incrementAndGet()
                        expectedSum.incrementAndGet()
                    }
                }
            }
        }

        threads.forEach { it.join() }

        val finalValue = cache.get("counter")?.get()
        assertThat(finalValue).isEqualTo(expectedSum.get())
    }
}
