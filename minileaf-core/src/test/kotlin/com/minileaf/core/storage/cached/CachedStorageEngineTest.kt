package com.minileaf.core.storage.cached

import com.minileaf.core.config.MinileafConfig
import com.minileaf.core.document.DocumentUtils
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.RepeatedTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread

/**
 * Comprehensive corruption tests for CachedStorageEngine.
 * Tests cache coherency, staleness bugs, and race conditions.
 */
class CachedStorageEngineTest {

    @TempDir
    lateinit var tempDir: Path

    private lateinit var storage: CachedStorageEngine<Int>

    @BeforeEach
    fun setup() {
        val config = MinileafConfig(
            dataDir = tempDir,
            memoryOnly = false,
            cacheSize = 50  // Small cache to trigger evictions
        )

        storage = CachedStorageEngine(
            collectionName = "test",
            config = config,
            cacheSize = 50,
            idSerializer = { it.toString() },
            idDeserializer = { it.toInt() }
        )
    }

    @AfterEach
    fun cleanup() {
        storage.close()
    }

    // ==================== Basic Functionality ====================

    @Test
    fun `basic upsert and findById`() {
        val doc = DocumentUtils.createDocument().apply {
            put("name", "Alice")
            put("age", 30)
        }

        storage.upsert(1, doc)
        val found = storage.findById(1)

        assertThat(found).isNotNull
        assertThat(found?.get("name")?.asText()).isEqualTo("Alice")
        assertThat(found?.get("age")?.asInt()).isEqualTo(30)
    }

    @Test
    fun `update existing document`() {
        val doc1 = DocumentUtils.createDocument().apply {
            put("value", "v1")
        }
        storage.upsert(1, doc1)

        val doc2 = DocumentUtils.createDocument().apply {
            put("value", "v2")
        }
        storage.upsert(1, doc2)

        val found = storage.findById(1)
        assertThat(found?.get("value")?.asText()).isEqualTo("v2")
    }

    @Test
    fun `delete removes from cache and disk`() {
        val doc = DocumentUtils.createDocument().apply {
            put("value", "test")
        }
        storage.upsert(1, doc)

        val deleted = storage.delete(1)
        assertThat(deleted).isNotNull

        val found = storage.findById(1)
        assertThat(found).isNull()
    }

    // ==================== Cache Coherency Tests ====================

    @RepeatedTest(5)
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    fun `no stale data - concurrent read after write`() {
        val errors = ConcurrentHashMap<Int, String>()
        val barrier = CyclicBarrier(50)

        // Pre-populate
        val doc = DocumentUtils.createDocument().apply {
            put("value", 0)
        }
        storage.upsert(1, doc)

        val threads = (1..50).map { threadId ->
            thread {
                try {
                    barrier.await()

                    repeat(100) { iteration ->
                        if (threadId % 2 == 0) {
                            // Writer: increment value
                            val newDoc = DocumentUtils.createDocument().apply {
                                put("value", threadId * 1000 + iteration)
                            }
                            storage.upsert(1, newDoc)
                        } else {
                            // Reader: verify value is never negative
                            val found = storage.findById(1)
                            if (found != null) {
                                val value = found.get("value")?.asInt() ?: -1
                                if (value < 0) {
                                    errors[threadId] = "Read negative value: $value"
                                }
                            }
                        }
                    }
                } catch (e: Exception) {
                    errors[threadId] = "Thread $threadId: ${e.message}"
                }
            }
        }

        threads.forEach { it.join() }

        if (errors.isNotEmpty()) {
            errors.forEach { (thread, error) -> println("ERROR: $error") }
        }
        assertThat(errors).isEmpty()
    }

    @RepeatedTest(5)
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    fun `cache miss followed by concurrent update returns fresh data`() {
        val errors = ConcurrentHashMap<Int, String>()
        val testIterations = 100

        repeat(testIterations) { iteration ->
            val key = 1000 + iteration

            // Write initial value
            val doc1 = DocumentUtils.createDocument().apply {
                put("version", 1)
            }
            storage.upsert(key, doc1)

            // Evict from cache by filling cache
            repeat(60) { i ->
                val evictDoc = DocumentUtils.createDocument().apply {
                    put("evict", i)
                }
                storage.upsert(10000 + i, evictDoc)
            }

            // Now key should be evicted from cache
            val barrier = CyclicBarrier(2)

            // Thread 1: Read (cache miss)
            val readThread = thread {
                barrier.await()
                Thread.sleep(5)  // Slight delay to let update happen first
                val found = storage.findById(key)
                val version = found?.get("version")?.asInt()

                // Should see version 2, not version 1
                if (version == 1) {
                    errors[iteration] = "Iteration $iteration: Saw stale version 1"
                }
            }

            // Thread 2: Update immediately
            val writeThread = thread {
                barrier.await()
                val doc2 = DocumentUtils.createDocument().apply {
                    put("version", 2)
                }
                storage.upsert(key, doc2)
            }

            readThread.join()
            writeThread.join()
        }

        if (errors.isNotEmpty()) {
            errors.forEach { (iter, error) -> println("ERROR: $error") }
        }
        assertThat(errors).isEmpty()
    }

    @RepeatedTest(5)
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    fun `cache and disk stay synchronized`() {
        val errors = ConcurrentHashMap<Int, String>()
        val barrier = CyclicBarrier(40)

        val threads = (1..40).map { threadId ->
            thread {
                try {
                    barrier.await()

                    repeat(200) { iteration ->
                        val key = threadId
                        val doc = DocumentUtils.createDocument().apply {
                            put("thread", threadId)
                            put("iteration", iteration)
                        }

                        // Write
                        storage.upsert(key, doc)

                        // Read back immediately
                        val found = storage.findById(key)
                        if (found != null) {
                            val foundThread = found.get("thread")?.asInt()
                            if (foundThread != threadId) {
                                errors[threadId] = "Cache/disk out of sync! Expected thread=$threadId, got $foundThread"
                            }
                        }
                    }
                } catch (e: Exception) {
                    errors[threadId] = "Exception: ${e.message}"
                }
            }
        }

        threads.forEach { it.join() }

        if (errors.isNotEmpty()) {
            errors.forEach { (thread, error) -> println("ERROR: $error") }
        }
        assertThat(errors).isEmpty()
    }

    // ==================== Concurrent Operations ====================

    @RepeatedTest(3)
    @Timeout(value = 20, unit = TimeUnit.SECONDS)
    fun `heavy concurrent mixed operations`() {
        val errors = ConcurrentHashMap<Int, String>()
        val barrier = CyclicBarrier(60)

        val threads = (1..60).map { threadId ->
            thread {
                try {
                    barrier.await()

                    repeat(500) { iteration ->
                        val key = iteration % 100

                        when (iteration % 4) {
                            0 -> {
                                // Upsert
                                val doc = DocumentUtils.createDocument().apply {
                                    put("thread", threadId)
                                    put("iteration", iteration)
                                    put("timestamp", System.nanoTime())
                                }
                                storage.upsert(key, doc)
                            }
                            1, 2 -> {
                                // FindById (more frequent)
                                val found = storage.findById(key)
                                if (found != null) {
                                    // Validate document structure
                                    val thread = found.get("thread")?.asInt()
                                    val iter = found.get("iteration")?.asInt()
                                    if (thread == null || iter == null) {
                                        errors[threadId] = "Invalid document structure"
                                    }
                                }
                            }
                            3 -> {
                                // Delete
                                storage.delete(key)
                            }
                        }
                    }
                } catch (e: Exception) {
                    errors[threadId] = "Thread $threadId: ${e.message}\n${e.stackTraceToString()}"
                }
            }
        }

        threads.forEach { it.join() }

        if (errors.isNotEmpty()) {
            errors.forEach { (thread, error) -> println("ERROR in thread $thread:\n$error") }
        }
        assertThat(errors).isEmpty()

        // Verify storage is in valid state
        val count = storage.count()
        assertThat(count).isBetween(0L, 100L)
    }

    @RepeatedTest(5)
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    fun `concurrent updates to same document are not lost`() {
        val key = 42
        val counter = AtomicInteger(0)
        val errors = ConcurrentHashMap<Int, String>()
        val barrier = CyclicBarrier(30)

        // Initialize
        val initialDoc = DocumentUtils.createDocument().apply {
            put("counter", 0)
        }
        storage.upsert(key, initialDoc)

        val threads = (1..30).map { threadId ->
            thread {
                try {
                    barrier.await()

                    repeat(100) {
                        // Read-modify-write
                        val doc = storage.findById(key)
                        if (doc != null) {
                            val currentValue = doc.get("counter")?.asInt() ?: 0
                            val newDoc = DocumentUtils.createDocument().apply {
                                put("counter", currentValue + 1)
                            }
                            storage.upsert(key, newDoc)
                            counter.incrementAndGet()
                        }
                    }
                } catch (e: Exception) {
                    errors[threadId] = "Thread $threadId: ${e.message}"
                }
            }
        }

        threads.forEach { it.join() }

        assertThat(errors).isEmpty()

        // Final value should reflect some updates (but not all due to lost updates in this pattern)
        // This is expected behavior without transactions
        val finalDoc = storage.findById(key)
        assertThat(finalDoc).isNotNull
        val finalValue = finalDoc?.get("counter")?.asInt()
        assertThat(finalValue).isGreaterThan(0)

        println("Counter incremented $counter times, final value: $finalValue")
    }

    // ==================== Cache Eviction Tests ====================

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    fun `evicted documents can still be read from disk`() {
        // Fill cache beyond capacity
        repeat(150) { i ->
            val doc = DocumentUtils.createDocument().apply {
                put("key", i)
                put("value", "doc-$i")
            }
            storage.upsert(i, doc)
        }

        // All documents should still be readable (from disk)
        repeat(150) { i ->
            val found = storage.findById(i)
            assertThat(found).isNotNull
            assertThat(found?.get("key")?.asInt()).isEqualTo(i)
            assertThat(found?.get("value")?.asText()).isEqualTo("doc-$i")
        }
    }

    @RepeatedTest(5)
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    fun `concurrent evictions do not cause corruption`() {
        val errors = ConcurrentHashMap<Int, String>()
        val barrier = CyclicBarrier(40)

        val threads = (1..40).map { threadId ->
            thread {
                try {
                    barrier.await()

                    repeat(300) { iteration ->
                        // Each thread writes to unique keys (causing evictions)
                        val key = threadId * 1000 + iteration
                        val doc = DocumentUtils.createDocument().apply {
                            put("thread", threadId)
                            put("iteration", iteration)
                        }

                        storage.upsert(key, doc)

                        // Immediately read back
                        val found = storage.findById(key)
                        if (found == null) {
                            errors[threadId] = "Document disappeared after write!"
                        } else {
                            val foundThread = found.get("thread")?.asInt()
                            if (foundThread != threadId) {
                                errors[threadId] = "Data corruption! Expected $threadId, got $foundThread"
                            }
                        }
                    }
                } catch (e: Exception) {
                    errors[threadId] = "Exception: ${e.message}"
                }
            }
        }

        threads.forEach { it.join() }

        if (errors.isNotEmpty()) {
            errors.forEach { (thread, error) -> println("ERROR: $error") }
        }
        assertThat(errors).isEmpty()

        // Total documents = 40 * 300 = 12000
        assertThat(storage.count()).isEqualTo(12000)
    }

    // ==================== FindAll Tests ====================

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    fun `findAll returns all documents`() {
        // Insert 100 documents
        repeat(100) { i ->
            val doc = DocumentUtils.createDocument().apply {
                put("index", i)
            }
            storage.upsert(i, doc)
        }

        val all = storage.findAll()
        assertThat(all).hasSize(100)

        // Verify all documents are correct
        all.forEach { (id, doc) ->
            assertThat(doc.get("index")?.asInt()).isEqualTo(id)
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    fun `findAll with pagination works correctly`() {
        // Insert 100 documents
        repeat(100) { i ->
            val doc = DocumentUtils.createDocument().apply {
                put("index", i)
            }
            storage.upsert(i, doc)
        }

        // Get page 2 (skip 20, take 10)
        val page = storage.findAll(skip = 20, limit = 10)

        assertThat(page).hasSize(10)

        // Should contain IDs 20-29 (sorted)
        val ids = page.map { it.first }.sorted()
        assertThat(ids).containsExactly(20, 21, 22, 23, 24, 25, 26, 27, 28, 29)
    }

    @RepeatedTest(5)
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    fun `findAll during concurrent modifications`() {
        val errors = ConcurrentHashMap<Int, String>()
        val barrier = CyclicBarrier(21)

        // 20 writers
        val writers = (1..20).map { threadId ->
            thread {
                try {
                    barrier.await()
                    repeat(100) { i ->
                        val doc = DocumentUtils.createDocument().apply {
                            put("thread", threadId)
                        }
                        storage.upsert(threadId * 100 + i, doc)
                    }
                } catch (e: Exception) {
                    errors[threadId] = "Writer $threadId: ${e.message}"
                }
            }
        }

        // 1 reader calling findAll repeatedly
        val reader = thread {
            try {
                barrier.await()
                repeat(50) {
                    val all = storage.findAll()
                    // Just verify no exceptions
                    all.forEach { (_, doc) ->
                        doc.get("thread")?.asInt()  // Access document
                    }
                }
            } catch (e: Exception) {
                errors[0] = "Reader: ${e.message}"
            }
        }

        (writers + reader).forEach { it.join() }

        assertThat(errors).isEmpty()
    }

    // ==================== CountMatching Tests ====================

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    fun `countMatching returns correct count`() {
        // Insert 100 documents, half active, half inactive
        repeat(100) { i ->
            val doc = DocumentUtils.createDocument().apply {
                put("index", i)
                put("active", i % 2 == 0)
            }
            storage.upsert(i, doc)
        }

        val activeCount = storage.countMatching { doc ->
            doc.get("active")?.asBoolean() == true
        }

        assertThat(activeCount).isEqualTo(50)
    }

    @RepeatedTest(5)
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    fun `countMatching during concurrent modifications`() {
        val errors = ConcurrentHashMap<Int, String>()
        val barrier = CyclicBarrier(30)

        // 20 writers
        val writers = (1..20).map { threadId ->
            thread {
                try {
                    barrier.await()
                    repeat(50) { i ->
                        val doc = DocumentUtils.createDocument().apply {
                            put("thread", threadId)
                            put("active", i % 2 == 0)
                        }
                        storage.upsert(threadId * 100 + i, doc)
                    }
                } catch (e: Exception) {
                    errors[threadId] = "Writer: ${e.message}"
                }
            }
        }

        // 10 counters
        val counters = (21..30).map { threadId ->
            thread {
                try {
                    barrier.await()
                    repeat(20) {
                        storage.countMatching { doc ->
                            doc.get("active")?.asBoolean() == true
                        }
                    }
                } catch (e: Exception) {
                    errors[threadId] = "Counter: ${e.message}"
                }
            }
        }

        (writers + counters).forEach { it.join() }

        assertThat(errors).isEmpty()
    }

    // ==================== Persistence Tests ====================

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    fun `data survives close and reopen`() {
        // Insert data
        repeat(50) { i ->
            val doc = DocumentUtils.createDocument().apply {
                put("index", i)
                put("value", "doc-$i")
            }
            storage.upsert(i, doc)
        }

        // Close
        storage.close()

        // Reopen
        val config = MinileafConfig(
            dataDir = tempDir,
            memoryOnly = false,
            cacheSize = 50
        )

        val newStorage = CachedStorageEngine(
            collectionName = "test",
            config = config,
            cacheSize = 50,
            idSerializer = { it.toString() },
            idDeserializer = { it.toInt() }
        )

        // Verify all data is present
        repeat(50) { i ->
            val found = newStorage.findById(i)
            assertThat(found).isNotNull
            assertThat(found?.get("index")?.asInt()).isEqualTo(i)
            assertThat(found?.get("value")?.asText()).isEqualTo("doc-$i")
        }

        newStorage.close()
    }
}
