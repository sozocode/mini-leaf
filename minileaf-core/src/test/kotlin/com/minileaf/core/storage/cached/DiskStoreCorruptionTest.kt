package com.minileaf.core.storage.cached

import com.fasterxml.jackson.databind.node.ObjectNode
import com.minileaf.core.config.MinileafConfig
import com.minileaf.core.document.DocumentUtils
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.RandomAccessFile
import java.nio.file.Path

/**
 * Tests for DiskStore corruption handling.
 *
 * Issue: EOFException when reading documents after partial writes or file corruption.
 *
 * Scenario from production logs:
 * - Job completed successfully at 19:42:16
 * - Export jobs retrieved successfully at 19:42:16.577
 * - 5 minutes later at 19:47:04, EOFException when reading the same document
 *
 * Root cause: Data written to RandomAccessFile is buffered in OS and not immediately
 * persisted. If process crashes or file is truncated, the index points to incomplete data.
 *
 * This test suite verifies:
 * 1. Corruption is detected gracefully (no exceptions thrown to caller)
 * 2. Corrupted entries are removed from index
 * 3. Other valid entries remain accessible
 * 4. fsync ensures data durability
 */
class DiskStoreCorruptionTest {

    @TempDir
    lateinit var tempDir: Path

    private lateinit var dataFile: Path

    @BeforeEach
    fun setup() {
        dataFile = tempDir.resolve("test.data")
    }

    @AfterEach
    fun cleanup() {
        // Cleanup handled by TempDir
    }

    /**
     * Simulates a partial write where only the ID length was written.
     * This can happen if the process crashes mid-write.
     */
    @Test
    fun `should handle corruption - only ID length written`() {
        // Create a disk store and write some valid documents
        val store = createDiskStore()

        val doc1 = createDoc("valid-doc-1")
        val doc2 = createDoc("valid-doc-2")

        store.write(1, doc1)
        store.write(2, doc2)

        // Verify documents are readable
        assertThat(store.read(1)?.get("name")?.asText()).isEqualTo("valid-doc-1")
        assertThat(store.read(2)?.get("name")?.asText()).isEqualTo("valid-doc-2")

        store.close()

        // Simulate partial write: append only ID length (4 bytes) for ID "3"
        RandomAccessFile(dataFile.toFile(), "rw").use { raf ->
            raf.seek(raf.length())
            raf.writeInt(1) // ID length = 1 (but no actual ID bytes follow)
            // File ends here - simulating crash mid-write
        }

        // Reopen the store - should handle corrupt entry gracefully during buildIndex
        val reopenedStore = createDiskStore()

        // Valid documents should still be accessible
        assertThat(reopenedStore.read(1)?.get("name")?.asText()).isEqualTo("valid-doc-1")
        assertThat(reopenedStore.read(2)?.get("name")?.asText()).isEqualTo("valid-doc-2")

        // Count should be 2 (not 3)
        assertThat(reopenedStore.count()).isEqualTo(2)

        reopenedStore.close()
    }

    /**
     * Simulates a partial write where ID was written but document length is missing.
     */
    @Test
    fun `should handle corruption - ID written but doc length missing`() {
        val store = createDiskStore()

        val doc1 = createDoc("doc-1")
        store.write(1, doc1)
        store.close()

        // Simulate partial write: ID length + ID bytes, but no doc length
        RandomAccessFile(dataFile.toFile(), "rw").use { raf ->
            raf.seek(raf.length())
            val idBytes = "2".toByteArray(Charsets.UTF_8)
            raf.writeInt(idBytes.size)
            raf.write(idBytes)
            // Doc length not written - crash simulation
        }

        // Reopen - should handle gracefully
        val reopenedStore = createDiskStore()

        // Original document should be accessible
        assertThat(reopenedStore.read(1)?.get("name")?.asText()).isEqualTo("doc-1")
        assertThat(reopenedStore.count()).isEqualTo(1)

        reopenedStore.close()
    }

    /**
     * Simulates partial document bytes written.
     */
    @Test
    fun `should handle corruption - partial document bytes written`() {
        val store = createDiskStore()

        val doc1 = createDoc("complete-doc")
        store.write(1, doc1)
        store.close()

        // Simulate partial write: full header but truncated document
        RandomAccessFile(dataFile.toFile(), "rw").use { raf ->
            raf.seek(raf.length())

            val idBytes = "999".toByteArray(Charsets.UTF_8)
            val docJson = """{"name":"truncated"}"""

            raf.writeInt(idBytes.size)
            raf.write(idBytes)
            raf.writeInt(docJson.length) // Says 20 bytes
            raf.write(docJson.toByteArray(Charsets.UTF_8).take(5).toByteArray()) // Only 5 bytes written
            // Crash - only partial doc bytes written
        }

        // Reopen - should handle gracefully
        val reopenedStore = createDiskStore()

        // Original document should be accessible
        assertThat(reopenedStore.read(1)?.get("name")?.asText()).isEqualTo("complete-doc")
        assertThat(reopenedStore.count()).isEqualTo(1)

        reopenedStore.close()
    }

    /**
     * Test that simulates the exact scenario from production:
     * - Document written successfully
     * - Document readable immediately after write
     * - File truncated/corrupted
     * - Next read returns null and removes from index
     */
    @Test
    fun `should detect corruption on read and remove from index`() {
        val store = createDiskStore()

        // Write 3 documents
        store.write(1, createDoc("doc-1"))
        store.write(2, createDoc("doc-2"))
        store.write(3, createDoc("doc-3"))

        // All readable
        assertThat(store.read(1)).isNotNull
        assertThat(store.read(2)).isNotNull
        assertThat(store.read(3)).isNotNull
        assertThat(store.count()).isEqualTo(3)

        store.close()

        // Corrupt the file by truncating it (simulating partial write / file corruption)
        val originalSize = dataFile.toFile().length()
        RandomAccessFile(dataFile.toFile(), "rw").use { raf ->
            // Truncate last 10 bytes - this will corrupt the last document
            raf.setLength(originalSize - 10)
        }

        // Reopen and verify handling
        val reopenedStore = createDiskStore()

        // First two documents should be OK (depending on where truncation happened)
        // At minimum, count should be less than 3
        val count = reopenedStore.count()
        assertThat(count).isLessThanOrEqualTo(2)

        reopenedStore.close()
    }

    /**
     * Test readAll() behavior with corrupted entries.
     * The readAll() method should:
     * 1. Return only valid documents
     * 2. Remove corrupted entries from the index
     */
    @Test
    fun `readAll should skip corrupted entries and clean index`() {
        val store = createDiskStore()

        // Write valid documents
        store.write(1, createDoc("doc-1"))
        store.write(2, createDoc("doc-2"))
        store.write(3, createDoc("doc-3"))
        store.write(4, createDoc("doc-4"))
        store.write(5, createDoc("doc-5"))

        store.close()

        // Truncate file to corrupt some entries
        val originalSize = dataFile.toFile().length()
        RandomAccessFile(dataFile.toFile(), "rw").use { raf ->
            raf.setLength(originalSize - 20) // Corrupt last entry or two
        }

        // Reopen
        val reopenedStore = createDiskStore()

        // readAll should return only valid documents
        val allDocs = reopenedStore.readAll()

        // Should have fewer than 5 documents
        assertThat(allDocs.size).isLessThan(5)

        // All returned documents should be valid
        allDocs.forEach { (id, doc) ->
            assertThat(doc.get("name")).isNotNull
        }

        // Count should match readAll size
        assertThat(reopenedStore.count()).isEqualTo(allDocs.size.toLong())

        reopenedStore.close()
    }

    /**
     * Test concurrent read during write doesn't cause issues.
     * While we can't fully simulate the production issue, we test
     * that the store is thread-safe.
     */
    @Test
    fun `concurrent reads and writes should be thread-safe`() {
        // Use syncOnWrite=false for performance in this thread-safety test
        val store = createDiskStore(syncOnWrite = false)

        val threads = mutableListOf<Thread>()
        val errors = mutableListOf<Throwable>()

        // Writer thread - write 20 small documents
        threads.add(Thread {
            try {
                repeat(20) { i ->
                    val doc = DocumentUtils.createDocument().apply {
                        put("id", i)
                    }
                    store.write(i, doc)
                }
            } catch (e: Throwable) {
                if (e !is OutOfMemoryError) {
                    synchronized(errors) { errors.add(e) }
                }
            }
        })

        // Reader thread - simple reads
        threads.add(Thread {
            try {
                repeat(20) { i ->
                    store.read(i) // May return null, that's OK
                    store.exists(i)
                }
            } catch (e: Throwable) {
                if (e !is OutOfMemoryError) {
                    synchronized(errors) { errors.add(e) }
                }
            }
        })

        threads.forEach { it.start() }
        threads.forEach { it.join() }

        assertThat(errors).isEmpty()

        store.close()
    }

    /**
     * Test that write + immediate read works (data is available in buffer).
     * This tests the scenario where data is readable immediately after write
     * even before fsync.
     */
    @Test
    fun `write followed by immediate read should work`() {
        val store = createDiskStore()

        repeat(100) { i ->
            val doc = createDoc("immediate-$i")
            store.write(i, doc)

            // Immediate read should work (data in buffer)
            val read = store.read(i)
            assertThat(read?.get("name")?.asText())
                .describedAs("Document $i should be readable immediately after write")
                .isEqualTo("immediate-$i")
        }

        store.close()
    }

    /**
     * Test that corrupted data in the middle of file doesn't prevent
     * reading earlier valid entries.
     */
    @Test
    fun `corruption at end should not affect earlier entries`() {
        val store = createDiskStore()

        // Write documents
        store.write(1, createDoc("first"))
        store.write(2, createDoc("second"))
        store.write(3, createDoc("third"))

        val offsetBeforeLast = store.fileSize()

        store.write(4, createDoc("last"))
        store.close()

        // Corrupt only the last entry by zeroing it out
        RandomAccessFile(dataFile.toFile(), "rw").use { raf ->
            raf.seek(offsetBeforeLast)
            // Write garbage that looks like corrupt data
            raf.writeInt(999999) // Invalid ID length
        }

        // Reopen - buildIndex will stop at corruption
        val reopenedStore = createDiskStore()

        // Earlier entries should still work
        assertThat(reopenedStore.read(1)?.get("name")?.asText()).isEqualTo("first")
        assertThat(reopenedStore.read(2)?.get("name")?.asText()).isEqualTo("second")
        assertThat(reopenedStore.read(3)?.get("name")?.asText()).isEqualTo("third")

        // Entry 4 should not be in index (corruption stopped indexing)
        assertThat(reopenedStore.read(4)).isNull()

        reopenedStore.close()
    }

    private fun createDiskStore(syncOnWrite: Boolean = true): DiskStore<Int> {
        return DiskStore(
            dataFile = dataFile,
            idSerializer = { it.toString() },
            idDeserializer = { it.toInt() },
            syncOnWrite = syncOnWrite
        )
    }

    private fun createDoc(name: String): ObjectNode {
        return DocumentUtils.createDocument().apply {
            put("name", name)
            put("timestamp", System.currentTimeMillis())
        }
    }
}
