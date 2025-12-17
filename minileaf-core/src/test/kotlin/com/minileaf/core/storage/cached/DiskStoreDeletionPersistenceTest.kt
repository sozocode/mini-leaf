package com.minileaf.core.storage.cached

import com.minileaf.core.config.MinileafConfig
import com.minileaf.core.document.DocumentUtils
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path

/**
 * Tests for the deletion persistence bug where deleted documents reappear after restart.
 *
 * Issue: When using LRU cache with disk persistence, if a document is created and deleted,
 * then the application is shutdown and restarted, the deleted document would reappear.
 *
 * Root cause: DiskStore.buildIndex() was not tracking deletion markers across restarts.
 * The deleted documents were physically present in the disk file (lazy deletion), so they
 * would be re-indexed on restart.
 *
 * Fix: Write deletion markers ({} empty JSON objects) to disk when deleting, and
 * recognize these markers during index rebuild to properly exclude deleted documents.
 */
class DiskStoreDeletionPersistenceTest {

    @TempDir
    lateinit var tempDir: Path

    private lateinit var storage: CachedStorageEngine<Int>

    @BeforeEach
    fun setup() {
        val config = MinileafConfig(
            dataDir = tempDir,
            memoryOnly = false,
            cacheSize = 10  // Small cache size
        )

        storage = CachedStorageEngine(
            collectionName = "test",
            config = config,
            cacheSize = 10,
            idSerializer = { it.toString() },
            idDeserializer = { it.toInt() }
        )
    }

    @AfterEach
    fun cleanup() {
        storage.close()
    }

    @Test
    fun `deleted document should not reappear after restart`() {
        // Step 1: Create a document
        val doc = DocumentUtils.createDocument().apply {
            put("name", "John Doe")
            put("age", 30)
        }
        storage.upsert(1, doc)

        // Verify it exists
        val found1 = storage.findById(1)
        assertThat(found1).isNotNull
        assertThat(found1?.get("name")?.asText()).isEqualTo("John Doe")

        // Step 2: Delete the document
        val deleted = storage.delete(1)
        assertThat(deleted).isNotNull
        assertThat(deleted?.get("name")?.asText()).isEqualTo("John Doe")

        // Verify it's gone
        val found2 = storage.findById(1)
        assertThat(found2).isNull()

        // Step 3: Close and reopen (simulating application restart)
        storage.close()

        val config = MinileafConfig(
            dataDir = tempDir,
            memoryOnly = false,
            cacheSize = 10
        )

        val newStorage = CachedStorageEngine(
            collectionName = "test",
            config = config,
            cacheSize = 10,
            idSerializer = { it.toString() },
            idDeserializer = { it.toInt() }
        )

        // Step 4: Verify document is still deleted (BUG: it would reappear before fix)
        val found3 = newStorage.findById(1)
        assertThat(found3)
            .describedAs("Deleted document should not reappear after restart")
            .isNull()

        // Verify count is 0
        assertThat(newStorage.count()).isEqualTo(0)

        newStorage.close()
    }

    @Test
    fun `multiple create-delete cycles persist correctly across restarts`() {
        // Create and delete document 3 times
        repeat(3) { cycle ->
            val doc = DocumentUtils.createDocument().apply {
                put("cycle", cycle)
                put("data", "cycle-$cycle")
            }
            storage.upsert(1, doc)

            // Verify exists
            assertThat(storage.findById(1)).isNotNull

            // Delete
            storage.delete(1)

            // Verify deleted
            assertThat(storage.findById(1)).isNull()

            // Close and reopen
            storage.close()

            val config = MinileafConfig(
                dataDir = tempDir,
                memoryOnly = false,
                cacheSize = 10
            )

            storage = CachedStorageEngine(
                collectionName = "test",
                config = config,
                cacheSize = 10,
                idSerializer = { it.toString() },
                idDeserializer = { it.toInt() }
            )

            // Verify still deleted after restart
            assertThat(storage.findById(1))
                .describedAs("Document should remain deleted after restart in cycle $cycle")
                .isNull()
        }
    }

    @Test
    fun `delete-recreate-delete sequence persists correctly`() {
        // Create document
        val doc1 = DocumentUtils.createDocument().apply {
            put("version", 1)
        }
        storage.upsert(1, doc1)

        // Delete
        storage.delete(1)
        assertThat(storage.findById(1)).isNull()

        // Recreate with different data
        val doc2 = DocumentUtils.createDocument().apply {
            put("version", 2)
        }
        storage.upsert(1, doc2)
        assertThat(storage.findById(1)?.get("version")?.asInt()).isEqualTo(2)

        // Delete again
        storage.delete(1)
        assertThat(storage.findById(1)).isNull()

        // Restart
        storage.close()

        val config = MinileafConfig(
            dataDir = tempDir,
            memoryOnly = false,
            cacheSize = 10
        )

        val newStorage = CachedStorageEngine(
            collectionName = "test",
            config = config,
            cacheSize = 10,
            idSerializer = { it.toString() },
            idDeserializer = { it.toInt() }
        )

        // Should still be deleted
        assertThat(newStorage.findById(1)).isNull()

        newStorage.close()
    }

    @Test
    fun `some deleted some active documents persist correctly across restart`() {
        // Create 10 documents
        repeat(10) { i ->
            val doc = DocumentUtils.createDocument().apply {
                put("id", i)
                put("value", "doc-$i")
            }
            storage.upsert(i, doc)
        }

        // Delete even-numbered documents (0, 2, 4, 6, 8)
        listOf(0, 2, 4, 6, 8).forEach { id ->
            storage.delete(id)
        }

        // Verify state before restart
        listOf(0, 2, 4, 6, 8).forEach { id ->
            assertThat(storage.findById(id)).isNull()
        }
        listOf(1, 3, 5, 7, 9).forEach { id ->
            assertThat(storage.findById(id)).isNotNull
        }

        // Restart
        storage.close()

        val config = MinileafConfig(
            dataDir = tempDir,
            memoryOnly = false,
            cacheSize = 10
        )

        val newStorage = CachedStorageEngine(
            collectionName = "test",
            config = config,
            cacheSize = 10,
            idSerializer = { it.toString() },
            idDeserializer = { it.toInt() }
        )

        // Verify deleted documents are still deleted
        listOf(0, 2, 4, 6, 8).forEach { id ->
            assertThat(newStorage.findById(id))
                .describedAs("Document $id should remain deleted after restart")
                .isNull()
        }

        // Verify active documents are still active
        listOf(1, 3, 5, 7, 9).forEach { id ->
            val found = newStorage.findById(id)
            assertThat(found)
                .describedAs("Document $id should remain active after restart")
                .isNotNull
            assertThat(found?.get("id")?.asInt()).isEqualTo(id)
        }

        // Verify count
        assertThat(newStorage.count()).isEqualTo(5)

        newStorage.close()
    }

    @Test
    fun `deletion persists even after cache eviction`() {
        // Create a document
        val doc = DocumentUtils.createDocument().apply {
            put("data", "important")
        }
        storage.upsert(1, doc)

        // Delete it
        storage.delete(1)

        // Evict from cache by inserting many other documents
        repeat(100) { i ->
            val evictDoc = DocumentUtils.createDocument().apply {
                put("evict", i)
            }
            storage.upsert(1000 + i, evictDoc)
        }

        // Verify still deleted (should read from disk)
        assertThat(storage.findById(1)).isNull()

        // Restart
        storage.close()

        val config = MinileafConfig(
            dataDir = tempDir,
            memoryOnly = false,
            cacheSize = 10
        )

        val newStorage = CachedStorageEngine(
            collectionName = "test",
            config = config,
            cacheSize = 10,
            idSerializer = { it.toString() },
            idDeserializer = { it.toInt() }
        )

        // Should still be deleted
        assertThat(newStorage.findById(1)).isNull()

        // Other documents should still exist
        assertThat(newStorage.count()).isEqualTo(100)

        newStorage.close()
    }

    @Test
    fun `deletion after compaction persists correctly`() {
        // Create and delete several documents
        repeat(5) { i ->
            val doc = DocumentUtils.createDocument().apply {
                put("temp", i)
            }
            storage.upsert(i, doc)
        }

        repeat(5) { i ->
            storage.delete(i)
        }

        // Compact to clean up deleted documents
        storage.compact()

        // Create a new document
        val doc = DocumentUtils.createDocument().apply {
            put("final", "data")
        }
        storage.upsert(10, doc)

        // Delete it
        storage.delete(10)

        // Restart
        storage.close()

        val config = MinileafConfig(
            dataDir = tempDir,
            memoryOnly = false,
            cacheSize = 10
        )

        val newStorage = CachedStorageEngine(
            collectionName = "test",
            config = config,
            cacheSize = 10,
            idSerializer = { it.toString() },
            idDeserializer = { it.toInt() }
        )

        // All documents should be deleted
        repeat(5) { i ->
            assertThat(newStorage.findById(i)).isNull()
        }
        assertThat(newStorage.findById(10)).isNull()
        assertThat(newStorage.count()).isEqualTo(0)

        newStorage.close()
    }
}
