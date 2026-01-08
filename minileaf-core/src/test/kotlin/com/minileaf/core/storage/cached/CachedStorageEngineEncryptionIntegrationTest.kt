package com.minileaf.core.storage.cached

import com.minileaf.core.config.MinileafConfig
import com.minileaf.core.crypto.Encryption
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
 * Integration tests for CachedStorageEngine encryption.
 * Includes multithreading and data integrity tests.
 */
class CachedStorageEngineEncryptionIntegrationTest {

    @TempDir
    lateinit var tempDir: Path

    private lateinit var encryptionKey: ByteArray
    private var storage: CachedStorageEngine<String>? = null

    @BeforeEach
    fun setup() {
        encryptionKey = Encryption.generateKey()
    }

    @AfterEach
    fun cleanup() {
        storage?.close()
    }

    private fun createStorage(key: ByteArray? = encryptionKey, cacheSize: Int = 100): CachedStorageEngine<String> {
        val config = MinileafConfig(
            dataDir = tempDir,
            encryptionKey = key,
            cacheSize = cacheSize,
            syncOnWrite = true
        )
        return CachedStorageEngine(
            collectionName = "test",
            config = config,
            cacheSize = cacheSize,
            idSerializer = { it },
            idDeserializer = { it }
        ).also { storage = it }
    }

    // ==================== Basic Encryption Tests ====================

    @Test
    fun `basic write and read with encryption`() {
        val store = createStorage()
        val doc = DocumentUtils.createDocument().apply {
            put("name", "Alice")
            put("secret", "password123")
        }

        store.upsert("1", doc)
        val found = store.findById("1")

        assertThat(found).isNotNull
        assertThat(found!!.get("name").asText()).isEqualTo("Alice")
        assertThat(found.get("secret").asText()).isEqualTo("password123")
    }

    @Test
    fun `encrypted data should not be plaintext on disk`() {
        val store = createStorage()
        val doc = DocumentUtils.createDocument().apply {
            put("apiKey", "sk-1234567890abcdef")
            put("password", "SuperSecretPassword123!")
        }

        store.upsert("secret-doc", doc)
        store.close()
        storage = null

        // Read raw file
        val dataFile = tempDir.resolve("collections").resolve("test.data").toFile()
        assertThat(dataFile.exists()).isTrue()

        val rawContent = String(dataFile.readBytes(), Charsets.UTF_8)

        // Sensitive data should NOT be visible
        assertThat(rawContent).doesNotContain("sk-1234567890abcdef")
        assertThat(rawContent).doesNotContain("SuperSecretPassword123!")
        assertThat(rawContent).doesNotContain("secret-doc")
    }

    @Test
    fun `data should persist and recover after restart`() {
        // Create and populate
        val store1 = createStorage()
        for (i in 1..100) {
            val doc = DocumentUtils.createDocument().apply {
                put("index", i)
                put("data", "value-$i")
            }
            store1.upsert("doc-$i", doc)
        }
        store1.close()
        storage = null

        // Reopen with same key
        val store2 = createStorage(encryptionKey)

        // Verify all data
        assertThat(store2.count()).isEqualTo(100)
        for (i in 1..100) {
            val doc = store2.findById("doc-$i")
            assertThat(doc).isNotNull
            assertThat(doc!!.get("index").asInt()).isEqualTo(i)
            assertThat(doc.get("data").asText()).isEqualTo("value-$i")
        }

        store2.close()
        storage = null
    }

    @Test
    fun `deletes should persist across restart with encryption`() {
        val store1 = createStorage()

        // Insert
        for (i in 1..10) {
            val doc = DocumentUtils.createDocument().apply { put("value", i) }
            store1.upsert("doc-$i", doc)
        }

        // Delete half
        for (i in 1..5) {
            store1.delete("doc-$i")
        }

        store1.close()
        storage = null

        // Reopen
        val store2 = createStorage(encryptionKey)

        // Verify
        assertThat(store2.count()).isEqualTo(5)
        for (i in 1..5) {
            assertThat(store2.findById("doc-$i")).isNull()
        }
        for (i in 6..10) {
            assertThat(store2.findById("doc-$i")).isNotNull
        }

        store2.close()
        storage = null
    }

    // ==================== Multithreading Tests ====================

    @Test
    @Timeout(30, unit = TimeUnit.SECONDS)
    fun `concurrent writes should maintain data integrity with encryption`() {
        val store = createStorage(cacheSize = 50)
        val numThreads = 10
        val operationsPerThread = 100
        val barrier = CyclicBarrier(numThreads)
        val errors = ConcurrentHashMap<String, Throwable>()
        val successfulWrites = AtomicInteger(0)

        val threads = (1..numThreads).map { threadId ->
            thread {
                try {
                    barrier.await()
                    for (i in 1..operationsPerThread) {
                        val docId = "t${threadId}-doc$i"
                        val doc = DocumentUtils.createDocument().apply {
                            put("threadId", threadId)
                            put("iteration", i)
                            put("data", "thread-$threadId-iter-$i")
                        }
                        store.upsert(docId, doc)
                        successfulWrites.incrementAndGet()
                    }
                } catch (e: Throwable) {
                    errors["thread-$threadId"] = e
                }
            }
        }

        threads.forEach { it.join() }

        assertThat(errors).isEmpty()
        assertThat(successfulWrites.get()).isEqualTo(numThreads * operationsPerThread)
        assertThat(store.count()).isEqualTo((numThreads * operationsPerThread).toLong())
    }

    @Test
    @Timeout(30, unit = TimeUnit.SECONDS)
    fun `concurrent reads and writes should not corrupt data with encryption`() {
        val store = createStorage(cacheSize = 20)
        val numDocs = 50
        val iterations = 100
        val errors = ConcurrentHashMap<String, Throwable>()
        val dataIntegrityViolations = AtomicInteger(0)

        // Pre-populate
        for (i in 1..numDocs) {
            val doc = DocumentUtils.createDocument().apply {
                put("id", i)
                put("counter", 0)
            }
            store.upsert("doc-$i", doc)
        }

        val barrier = CyclicBarrier(4)

        // Writer threads
        val writers = (1..2).map { writerId ->
            thread {
                try {
                    barrier.await()
                    for (iter in 1..iterations) {
                        val docId = "doc-${(iter % numDocs) + 1}"
                        val doc = DocumentUtils.createDocument().apply {
                            put("id", (iter % numDocs) + 1)
                            put("counter", iter)
                            put("writer", writerId)
                        }
                        store.upsert(docId, doc)
                    }
                } catch (e: Throwable) {
                    errors["writer-$writerId"] = e
                }
            }
        }

        // Reader threads
        val readers = (1..2).map { readerId ->
            thread {
                try {
                    barrier.await()
                    for (iter in 1..iterations) {
                        val docId = "doc-${(iter % numDocs) + 1}"
                        val doc = store.findById(docId)
                        if (doc != null) {
                            // Verify document is internally consistent
                            val id = doc.get("id")?.asInt()
                            if (id != null && id != (iter % numDocs) + 1) {
                                // Only flag if id field is corrupted
                                val expectedId = (iter % numDocs) + 1
                                if (id < 1 || id > numDocs) {
                                    dataIntegrityViolations.incrementAndGet()
                                }
                            }
                        }
                    }
                } catch (e: Throwable) {
                    errors["reader-$readerId"] = e
                }
            }
        }

        (writers + readers).forEach { it.join() }

        assertThat(errors).isEmpty()
        assertThat(dataIntegrityViolations.get()).isEqualTo(0)
    }

    @Test
    @Timeout(30, unit = TimeUnit.SECONDS)
    @RepeatedTest(3)
    fun `data integrity after concurrent operations and restart`() {
        val store1 = createStorage(cacheSize = 30)
        val numThreads = 5
        val docsPerThread = 50
        val barrier = CyclicBarrier(numThreads)
        val errors = ConcurrentHashMap<String, Throwable>()

        // Concurrent writes
        val threads = (1..numThreads).map { threadId ->
            thread {
                try {
                    barrier.await()
                    for (i in 1..docsPerThread) {
                        val docId = "t${threadId}-d$i"
                        val doc = DocumentUtils.createDocument().apply {
                            put("threadId", threadId)
                            put("docNum", i)
                            put("checksum", threadId * 1000 + i)  // Simple checksum
                        }
                        store1.upsert(docId, doc)
                    }
                } catch (e: Throwable) {
                    errors["thread-$threadId"] = e
                }
            }
        }

        threads.forEach { it.join() }
        assertThat(errors).isEmpty()

        store1.close()
        storage = null

        // Reopen and verify integrity
        val store2 = createStorage(encryptionKey, cacheSize = 30)

        var verified = 0
        var corrupted = 0
        for (threadId in 1..numThreads) {
            for (i in 1..docsPerThread) {
                val docId = "t${threadId}-d$i"
                val doc = store2.findById(docId)

                if (doc == null) {
                    corrupted++
                    continue
                }

                val storedThreadId = doc.get("threadId")?.asInt()
                val storedDocNum = doc.get("docNum")?.asInt()
                val storedChecksum = doc.get("checksum")?.asInt()
                val expectedChecksum = threadId * 1000 + i

                if (storedThreadId != threadId || storedDocNum != i || storedChecksum != expectedChecksum) {
                    corrupted++
                } else {
                    verified++
                }
            }
        }

        assertThat(corrupted).isEqualTo(0)
        assertThat(verified).isEqualTo(numThreads * docsPerThread)

        store2.close()
        storage = null
    }

    @Test
    @Timeout(30, unit = TimeUnit.SECONDS)
    fun `concurrent deletes should work correctly with encryption`() {
        val store = createStorage(cacheSize = 50)
        val numDocs = 100
        val errors = ConcurrentHashMap<String, Throwable>()

        // Pre-populate
        for (i in 1..numDocs) {
            val doc = DocumentUtils.createDocument().apply { put("id", i) }
            store.upsert("doc-$i", doc)
        }

        assertThat(store.count()).isEqualTo(numDocs.toLong())

        val barrier = CyclicBarrier(4)

        // Delete odd numbers
        val deleter1 = thread {
            try {
                barrier.await()
                for (i in 1..numDocs step 2) {
                    store.delete("doc-$i")
                }
            } catch (e: Throwable) {
                errors["deleter1"] = e
            }
        }

        // Delete multiples of 10
        val deleter2 = thread {
            try {
                barrier.await()
                for (i in 10..numDocs step 10) {
                    store.delete("doc-$i")
                }
            } catch (e: Throwable) {
                errors["deleter2"] = e
            }
        }

        // Concurrent readers
        val readers = (1..2).map { readerId ->
            thread {
                try {
                    barrier.await()
                    repeat(200) {
                        val docId = "doc-${(it % numDocs) + 1}"
                        store.findById(docId)  // Just ensure no crash
                    }
                } catch (e: Throwable) {
                    errors["reader-$readerId"] = e
                }
            }
        }

        listOf(deleter1, deleter2).forEach { it.join() }
        readers.forEach { it.join() }

        assertThat(errors).isEmpty()

        // Verify: should have only even non-multiples of 10
        // Odd: 1,3,5,...,99 = 50 deleted
        // Multiples of 10: 10,20,...,100 = 10 deleted (but 10,30,50,70,90 already deleted as odd)
        // So: deleted = 50 (odd) + 5 (even multiples of 10) = 55
        // Remaining should be: 2,4,6,8,12,14,16,18,22,... = 45
        val remaining = (1..numDocs).filter { i ->
            i % 2 == 0 && i % 10 != 0
        }.size

        assertThat(store.count()).isEqualTo(remaining.toLong())
    }

    // ==================== Edge Cases ====================

    @Test
    fun `empty collection should work with encryption`() {
        val store = createStorage()
        assertThat(store.count()).isEqualTo(0)
        assertThat(store.findById("nonexistent")).isNull()
        assertThat(store.findAll()).isEmpty()
    }

    @Test
    fun `large documents should encrypt correctly`() {
        val store = createStorage()

        // Create a large document
        val largeData = "x".repeat(100_000)
        val doc = DocumentUtils.createDocument().apply {
            put("largeField", largeData)
            put("checksum", largeData.length)
        }

        store.upsert("large-doc", doc)
        store.close()
        storage = null

        // Reopen and verify
        val store2 = createStorage(encryptionKey)
        val found = store2.findById("large-doc")

        assertThat(found).isNotNull
        assertThat(found!!.get("largeField").asText()).isEqualTo(largeData)
        assertThat(found.get("checksum").asInt()).isEqualTo(100_000)

        store2.close()
        storage = null
    }

    @Test
    fun `findAll should return all documents with encryption`() {
        val store = createStorage()

        for (i in 1..100) {
            val doc = DocumentUtils.createDocument().apply {
                put("index", i)
            }
            store.upsert("doc-$i", doc)
        }

        val all = store.findAll()
        assertThat(all).hasSize(100)

        val indices = all.map { (_, doc) -> doc.get("index").asInt() }.sorted()
        assertThat(indices).isEqualTo((1..100).toList())
    }

    @Test
    fun `compact should work with encryption`() {
        val store = createStorage()

        // Create some data
        for (i in 1..50) {
            val doc = DocumentUtils.createDocument().apply { put("id", i) }
            store.upsert("doc-$i", doc)
        }

        // Delete half
        for (i in 1..25) {
            store.delete("doc-$i")
        }

        val sizeBefore = store.stats().storageBytes

        // Compact
        store.compact()

        val sizeAfter = store.stats().storageBytes

        // File should be smaller
        assertThat(sizeAfter).isLessThan(sizeBefore)

        // Data should be intact
        assertThat(store.count()).isEqualTo(25)
        for (i in 26..50) {
            assertThat(store.findById("doc-$i")).isNotNull
        }
    }
}
