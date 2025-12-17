package com.minileaf.core.storage

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.minileaf.core.config.MinileafConfig
import com.minileaf.core.storage.cached.CachedStorageEngine
import com.minileaf.core.storage.file.FileBackedStorageEngine
import com.minileaf.core.storage.memory.InMemoryStorageEngine
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.*
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path

/**
 * Comprehensive tests for atomic field updates across all storage engine types.
 * Tests all three modes: InMemory, FileBackedStorage, and CachedStorage.
 */
class AtomicUpdateStorageEngineTest {

    private val mapper = ObjectMapper()

    @Nested
    @DisplayName("InMemoryStorageEngine Atomic Updates")
    inner class InMemoryStorageEngineAtomicUpdateTest {

        private lateinit var storage: InMemoryStorageEngine<String>

        @BeforeEach
        fun setup() {
            storage = InMemoryStorageEngine()
        }

        @Test
        fun `${"$"}set updates fields atomically in memory`() {
            // Create document
            val doc = createDocument("user1", "Alice", 25, 0L)
            storage.upsert("id1", doc)

            // Update fields
            val updated = storage.updateFields(
                "id1",
                mapOf(
                    "\$set" to mapOf(
                        "name" to "Alice Smith",
                        "age" to 26
                    )
                )
            )

            assertThat(updated).isTrue()

            // Verify changes
            val result = storage.findById("id1")!!
            assertThat(result.get("name").asText()).isEqualTo("Alice Smith")
            assertThat(result.get("age").asInt()).isEqualTo(26)
            assertThat(result.get("email").asText()).isEqualTo("alice@example.com") // Unchanged
        }

        @Test
        fun `${"$"}inc increments numeric fields in memory`() {
            val doc = createDocument("user1", "Bob", 30, 10L)
            storage.upsert("id1", doc)

            // Increment visits
            storage.updateFields("id1", mapOf("\$inc" to mapOf("visits" to 5L)))

            var result = storage.findById("id1")!!
            assertThat(result.get("visits").asLong()).isEqualTo(15L)

            // Increment again
            storage.updateFields("id1", mapOf("\$inc" to mapOf("visits" to 3L)))

            result = storage.findById("id1")!!
            assertThat(result.get("visits").asLong()).isEqualTo(18L)
        }

        @Test
        fun `${"$"}unset removes fields in memory`() {
            val doc = createDocument("user1", "Charlie", 35, 100L)
            doc.put("temp", "value")
            storage.upsert("id1", doc)

            // Remove temp field
            storage.updateFields("id1", mapOf("\$unset" to mapOf("temp" to 1)))

            val result = storage.findById("id1")!!
            assertThat(result.has("temp")).isFalse()
            assertThat(result.has("name")).isTrue() // Other fields remain
        }

        @Test
        fun `combine multiple operators in memory`() {
            val doc = createDocument("user1", "David", 40, 100L)
            storage.upsert("id1", doc)

            storage.updateFields(
                "id1",
                mapOf(
                    "\$set" to mapOf("name" to "David Johnson", "age" to 41),
                    "\$inc" to mapOf("visits" to 10L)
                )
            )

            val result = storage.findById("id1")!!
            assertThat(result.get("name").asText()).isEqualTo("David Johnson")
            assertThat(result.get("age").asInt()).isEqualTo(41)
            assertThat(result.get("visits").asLong()).isEqualTo(110L)
        }

        @Test
        fun `update non-existent document returns false in memory`() {
            val updated = storage.updateFields("non-existent", mapOf("\$set" to mapOf("name" to "Nobody")))
            assertThat(updated).isFalse()
        }

        @Test
        fun `nested field updates work in memory`() {
            val doc = mapper.createObjectNode()
            doc.put("_id", "id1")
            val address = doc.putObject("address")
            address.put("city", "New York")
            address.put("zip", "10001")

            storage.upsert("id1", doc)

            // Update nested field
            storage.updateFields("id1", mapOf("\$set" to mapOf("address.city" to "San Francisco")))

            val result = storage.findById("id1")!!
            assertThat(result.get("address").get("city").asText()).isEqualTo("San Francisco")
            assertThat(result.get("address").get("zip").asText()).isEqualTo("10001") // Unchanged
        }

        @Test
        fun `${"$"}set can set fields to null in memory`() {
            val doc = createDocument("user1", "Grace", 32, 100L)
            doc.put("metadata", "some data")
            storage.upsert("id1", doc)

            // Set metadata to null
            val updated = storage.updateFields(
                "id1",
                mapOf("\$set" to mapOf("metadata" to null))
            )

            assertThat(updated).isTrue()

            val result = storage.findById("id1")!!
            assertThat(result.has("metadata")).isTrue()
            assertThat(result.get("metadata").isNull).isTrue()
            assertThat(result.get("name").asText()).isEqualTo("Grace") // Other fields unchanged
        }

        @Test
        fun `${"$"}set can set multiple fields including null in memory`() {
            val doc = createDocument("user1", "Henry", 45, 50L)
            doc.put("status", "active")
            doc.put("metadata", "data")
            storage.upsert("id1", doc)

            // Update name and set metadata to null
            val updated = storage.updateFields(
                "id1",
                mapOf(
                    "\$set" to mapOf(
                        "name" to "Henry Williams",
                        "metadata" to null,
                        "status" to "inactive"
                    )
                )
            )

            assertThat(updated).isTrue()

            val result = storage.findById("id1")!!
            assertThat(result.get("name").asText()).isEqualTo("Henry Williams")
            assertThat(result.get("status").asText()).isEqualTo("inactive")
            assertThat(result.get("metadata").isNull).isTrue()
            assertThat(result.get("age").asInt()).isEqualTo(45) // Unchanged
        }
    }

    @Nested
    @DisplayName("FileBackedStorageEngine Atomic Updates")
    inner class FileBackedStorageEngineAtomicUpdateTest {

        @TempDir
        lateinit var tempDir: Path

        private lateinit var storage: FileBackedStorageEngine<String>

        @BeforeEach
        fun setup() {
            val config = MinileafConfig(dataDir = tempDir, memoryOnly = false)
            storage = FileBackedStorageEngine(
                collectionName = "test",
                config = config,
                idSerializer = { it },
                idDeserializer = { it }
            )
        }

        @AfterEach
        fun cleanup() {
            storage.close()
        }

        @Test
        fun `${"$"}set updates fields atomically with WAL persistence`() {
            val doc = createDocument("user1", "Alice", 25, 0L)
            storage.upsert("id1", doc)

            storage.updateFields(
                "id1",
                mapOf("\$set" to mapOf("name" to "Alice Smith", "age" to 26))
            )

            val result = storage.findById("id1")!!
            assertThat(result.get("name").asText()).isEqualTo("Alice Smith")
            assertThat(result.get("age").asInt()).isEqualTo(26)
        }

        @Test
        fun `${"$"}inc increments with WAL persistence`() {
            val doc = createDocument("user1", "Bob", 30, 10L)
            storage.upsert("id1", doc)

            storage.updateFields("id1", mapOf("\$inc" to mapOf("visits" to 5L)))

            val result = storage.findById("id1")!!
            assertThat(result.get("visits").asLong()).isEqualTo(15L)
        }

        @Test
        fun `updates persist across storage engine restarts`() {
            val doc = createDocument("user1", "Eve", 28, 50L)
            storage.upsert("id1", doc)

            storage.updateFields(
                "id1",
                mapOf(
                    "\$set" to mapOf("name" to "Eve Anderson"),
                    "\$inc" to mapOf("visits" to 25L)
                )
            )

            // Close and reopen
            storage.close()

            val config = MinileafConfig(dataDir = tempDir, memoryOnly = false)
            storage = FileBackedStorageEngine(
                collectionName = "test",
                config = config,
                idSerializer = { it },
                idDeserializer = { it }
            )

            // Verify updates persisted
            val result = storage.findById("id1")!!
            assertThat(result.get("name").asText()).isEqualTo("Eve Anderson")
            assertThat(result.get("visits").asLong()).isEqualTo(75L)
        }

        @Test
        fun `multiple updates write to WAL correctly`() {
            val doc = createDocument("user1", "Frank", 20, 0L)
            storage.upsert("id1", doc)

            // Perform multiple updates
            for (i in 1..5) {
                storage.updateFields("id1", mapOf("\$inc" to mapOf("visits" to 1L)))
            }

            val result = storage.findById("id1")!!
            assertThat(result.get("visits").asLong()).isEqualTo(5L)
        }

        @Test
        fun `concurrent updates are thread-safe with file-backed storage`() {
            val doc = createDocument("user1", "Grace", 35, 0L)
            storage.upsert("id1", doc)

            // Simulate concurrent updates
            val threads = (1..10).map { threadNum ->
                Thread {
                    storage.updateFields("id1", mapOf("\$inc" to mapOf("visits" to 1L)))
                }
            }

            threads.forEach { it.start() }
            threads.forEach { it.join() }

            val result = storage.findById("id1")!!
            assertThat(result.get("visits").asLong()).isEqualTo(10L)
        }

        @Test
        fun `${"$"}set can set fields to null and persist to disk`() {
            val doc = createDocument("user1", "Isabel", 29, 200L)
            doc.put("metadata", "important data")
            doc.put("status", "active")
            storage.upsert("id1", doc)

            // Set metadata to null
            val updated = storage.updateFields(
                "id1",
                mapOf("\$set" to mapOf("metadata" to null))
            )

            assertThat(updated).isTrue()

            // Verify before restart
            var result = storage.findById("id1")!!
            assertThat(result.get("metadata").isNull).isTrue()
            assertThat(result.get("status").asText()).isEqualTo("active")

            // Close and reopen to verify persistence
            storage.close()

            val config = MinileafConfig(dataDir = tempDir, memoryOnly = false)
            storage = FileBackedStorageEngine(
                collectionName = "test",
                config = config,
                idSerializer = { it },
                idDeserializer = { it }
            )

            // Verify null value persisted
            result = storage.findById("id1")!!
            assertThat(result.has("metadata")).isTrue()
            assertThat(result.get("metadata").isNull).isTrue()
            assertThat(result.get("status").asText()).isEqualTo("active")
            assertThat(result.get("name").asText()).isEqualTo("Isabel")
        }

        @Test
        fun `${"$"}set can set multiple fields including null and persist`() {
            val doc = createDocument("user1", "Jack", 38, 150L)
            doc.put("status", "pending")
            doc.put("metadata", "some metadata")
            doc.put("tempField", "temporary")
            storage.upsert("id1", doc)

            // Update multiple fields with null values
            val updated = storage.updateFields(
                "id1",
                mapOf(
                    "\$set" to mapOf(
                        "name" to "Jack Thompson",
                        "status" to "approved",
                        "metadata" to null,
                        "tempField" to null
                    ),
                    "\$inc" to mapOf("visits" to 50L)
                )
            )

            assertThat(updated).isTrue()

            // Verify before restart
            var result = storage.findById("id1")!!
            assertThat(result.get("name").asText()).isEqualTo("Jack Thompson")
            assertThat(result.get("status").asText()).isEqualTo("approved")
            assertThat(result.get("metadata").isNull).isTrue()
            assertThat(result.get("tempField").isNull).isTrue()
            assertThat(result.get("visits").asLong()).isEqualTo(200L)

            // Close and reopen
            storage.close()

            val config = MinileafConfig(dataDir = tempDir, memoryOnly = false)
            storage = FileBackedStorageEngine(
                collectionName = "test",
                config = config,
                idSerializer = { it },
                idDeserializer = { it }
            )

            // Verify all updates including null values persisted
            result = storage.findById("id1")!!
            assertThat(result.get("name").asText()).isEqualTo("Jack Thompson")
            assertThat(result.get("status").asText()).isEqualTo("approved")
            assertThat(result.get("metadata").isNull).isTrue()
            assertThat(result.get("tempField").isNull).isTrue()
            assertThat(result.get("visits").asLong()).isEqualTo(200L)
            assertThat(result.get("age").asInt()).isEqualTo(38) // Unchanged
        }

        @Test
        fun `null values persist through snapshot and WAL recovery`() {
            val doc = createDocument("user1", "Kate", 27, 100L)
            doc.put("optionalField", "initial value")
            storage.upsert("id1", doc)

            // Set to null
            storage.updateFields("id1", mapOf("\$set" to mapOf("optionalField" to null)))

            // Force a snapshot
            storage.compact()

            // Do another update to write to WAL
            storage.updateFields("id1", mapOf("\$inc" to mapOf("visits" to 10L)))

            // Close and reopen (will replay WAL after loading snapshot)
            storage.close()

            val config = MinileafConfig(dataDir = tempDir, memoryOnly = false)
            storage = FileBackedStorageEngine(
                collectionName = "test",
                config = config,
                idSerializer = { it },
                idDeserializer = { it }
            )

            // Verify null value survived snapshot and WAL recovery
            val result = storage.findById("id1")!!
            assertThat(result.get("optionalField").isNull).isTrue()
            assertThat(result.get("visits").asLong()).isEqualTo(110L)
        }
    }

    @Nested
    @DisplayName("CachedStorageEngine Atomic Updates")
    inner class CachedStorageEngineAtomicUpdateTest {

        @TempDir
        lateinit var tempDir: Path

        private lateinit var storage: CachedStorageEngine<String>

        @BeforeEach
        fun setup() {
            val config = MinileafConfig(dataDir = tempDir, memoryOnly = false)
            storage = CachedStorageEngine(
                collectionName = "test",
                config = config,
                cacheSize = 100,
                idSerializer = { it },
                idDeserializer = { it }
            )
        }

        @AfterEach
        fun cleanup() {
            storage.close()
        }

        @Test
        fun `${"$"}set updates both cache and disk`() {
            val doc = createDocument("user1", "Alice", 25, 0L)
            storage.upsert("id1", doc)

            storage.updateFields(
                "id1",
                mapOf("\$set" to mapOf("name" to "Alice Smith", "age" to 26))
            )

            // Verify update in cache
            val result = storage.findById("id1")!!
            assertThat(result.get("name").asText()).isEqualTo("Alice Smith")
            assertThat(result.get("age").asInt()).isEqualTo(26)
        }

        @Test
        fun `${"$"}inc updates both cache and disk`() {
            val doc = createDocument("user1", "Bob", 30, 10L)
            storage.upsert("id1", doc)

            storage.updateFields("id1", mapOf("\$inc" to mapOf("visits" to 5L)))

            val result = storage.findById("id1")!!
            assertThat(result.get("visits").asLong()).isEqualTo(15L)
        }

        @Test
        fun `updates persist after cache eviction`() {
            // Fill cache beyond capacity to force eviction
            for (i in 1..150) {
                val doc = createDocument("user$i", "User $i", 25, 0L)
                storage.upsert("id$i", doc)
            }

            // Update first document (likely evicted from cache)
            storage.updateFields("id1", mapOf("\$set" to mapOf("name" to "Updated User 1")))

            // Retrieve and verify (will reload from disk)
            val result = storage.findById("id1")!!
            assertThat(result.get("name").asText()).isEqualTo("Updated User 1")
        }

        @Test
        fun `updates to cached documents are immediate`() {
            val doc = createDocument("user1", "Charlie", 35, 100L)
            storage.upsert("id1", doc)

            // First access loads to cache
            storage.findById("id1")

            // Update should hit cache
            storage.updateFields("id1", mapOf("\$inc" to mapOf("visits" to 25L)))

            // Immediate read should show updated value
            val result = storage.findById("id1")!!
            assertThat(result.get("visits").asLong()).isEqualTo(125L)
        }

        @Test
        fun `updates persist across storage restart`() {
            val doc = createDocument("user1", "David", 40, 100L)
            storage.upsert("id1", doc)

            storage.updateFields(
                "id1",
                mapOf(
                    "\$set" to mapOf("name" to "David Johnson"),
                    "\$inc" to mapOf("visits" to 50L)
                )
            )

            // Close and reopen
            storage.close()

            val config = MinileafConfig(dataDir = tempDir, memoryOnly = false)
            storage = CachedStorageEngine(
                collectionName = "test",
                config = config,
                cacheSize = 100,
                idSerializer = { it },
                idDeserializer = { it }
            )

            // Verify updates persisted
            val result = storage.findById("id1")!!
            assertThat(result.get("name").asText()).isEqualTo("David Johnson")
            assertThat(result.get("visits").asLong()).isEqualTo(150L)
        }

        @Test
        fun `concurrent updates with LRU cache are thread-safe`() {
            val doc = createDocument("user1", "Eve", 28, 0L)
            storage.upsert("id1", doc)

            // Concurrent increments
            val threads = (1..20).map {
                Thread {
                    storage.updateFields("id1", mapOf("\$inc" to mapOf("visits" to 1L)))
                }
            }

            threads.forEach { it.start() }
            threads.forEach { it.join() }

            val result = storage.findById("id1")!!
            assertThat(result.get("visits").asLong()).isEqualTo(20L)
        }

        @Test
        fun `nested field updates work with cached storage`() {
            val doc = mapper.createObjectNode()
            doc.put("_id", "id1")
            val metadata = doc.putObject("metadata")
            metadata.put("score", 100)
            metadata.put("level", 5)

            storage.upsert("id1", doc)

            // Update nested field
            storage.updateFields(
                "id1",
                mapOf(
                    "\$set" to mapOf("metadata.level" to 6),
                    "\$inc" to mapOf("metadata.score" to 50)
                )
            )

            val result = storage.findById("id1")!!
            assertThat(result.get("metadata").get("level").asInt()).isEqualTo(6)
            assertThat(result.get("metadata").get("score").asInt()).isEqualTo(150)
        }

        @Test
        fun `${"$"}set can set fields to null in cache and persist to disk`() {
            val doc = createDocument("user1", "Laura", 31, 75L)
            doc.put("metadata", "cached data")
            doc.put("status", "online")
            storage.upsert("id1", doc)

            // Set metadata to null
            val updated = storage.updateFields(
                "id1",
                mapOf("\$set" to mapOf("metadata" to null))
            )

            assertThat(updated).isTrue()

            // Verify in cache
            var result = storage.findById("id1")!!
            assertThat(result.get("metadata").isNull).isTrue()
            assertThat(result.get("status").asText()).isEqualTo("online")

            // Close and reopen to verify disk persistence
            storage.close()

            val config = MinileafConfig(dataDir = tempDir, memoryOnly = false)
            storage = CachedStorageEngine(
                collectionName = "test",
                config = config,
                cacheSize = 100,
                idSerializer = { it },
                idDeserializer = { it }
            )

            // Verify null value persisted to disk
            result = storage.findById("id1")!!
            assertThat(result.has("metadata")).isTrue()
            assertThat(result.get("metadata").isNull).isTrue()
            assertThat(result.get("status").asText()).isEqualTo("online")
            assertThat(result.get("name").asText()).isEqualTo("Laura")
        }

        @Test
        fun `${"$"}set null values persist after cache eviction`() {
            val doc = createDocument("user1", "Mike", 42, 300L)
            doc.put("optionalData", "will be null")
            storage.upsert("id1", doc)

            // Set to null
            storage.updateFields("id1", mapOf("\$set" to mapOf("optionalData" to null)))

            // Fill cache to force eviction of id1
            for (i in 2..150) {
                val otherDoc = createDocument("user$i", "User $i", 25, 0L)
                storage.upsert("id$i", otherDoc)
            }

            // Retrieve id1 (should reload from disk since likely evicted)
            val result = storage.findById("id1")!!
            assertThat(result.get("optionalData").isNull).isTrue()
            assertThat(result.get("name").asText()).isEqualTo("Mike")
        }

        @Test
        fun `${"$"}set multiple null values persist across restart with cache`() {
            val doc = createDocument("user1", "Nancy", 36, 180L)
            doc.put("field1", "value1")
            doc.put("field2", "value2")
            doc.put("field3", "value3")
            storage.upsert("id1", doc)

            // Update with multiple null values
            val updated = storage.updateFields(
                "id1",
                mapOf(
                    "\$set" to mapOf(
                        "name" to "Nancy Brown",
                        "field1" to null,
                        "field2" to null,
                        "field3" to "updated"
                    ),
                    "\$inc" to mapOf("visits" to 20L)
                )
            )

            assertThat(updated).isTrue()

            // Verify before restart
            var result = storage.findById("id1")!!
            assertThat(result.get("name").asText()).isEqualTo("Nancy Brown")
            assertThat(result.get("field1").isNull).isTrue()
            assertThat(result.get("field2").isNull).isTrue()
            assertThat(result.get("field3").asText()).isEqualTo("updated")
            assertThat(result.get("visits").asLong()).isEqualTo(200L)

            // Close and reopen
            storage.close()

            val config = MinileafConfig(dataDir = tempDir, memoryOnly = false)
            storage = CachedStorageEngine(
                collectionName = "test",
                config = config,
                cacheSize = 100,
                idSerializer = { it },
                idDeserializer = { it }
            )

            // Verify all null values persisted
            result = storage.findById("id1")!!
            assertThat(result.get("name").asText()).isEqualTo("Nancy Brown")
            assertThat(result.get("field1").isNull).isTrue()
            assertThat(result.get("field2").isNull).isTrue()
            assertThat(result.get("field3").asText()).isEqualTo("updated")
            assertThat(result.get("visits").asLong()).isEqualTo(200L)
            assertThat(result.get("age").asInt()).isEqualTo(36) // Unchanged
        }

        @Test
        fun `null values in cache survive concurrent updates`() {
            val doc = createDocument("user1", "Oliver", 33, 0L)
            doc.put("counter", 0)
            doc.put("metadata", "will be null")
            storage.upsert("id1", doc)

            // Set metadata to null first
            storage.updateFields("id1", mapOf("\$set" to mapOf("metadata" to null)))

            // Concurrent increments while metadata is null
            val threads = (1..10).map {
                Thread {
                    storage.updateFields("id1", mapOf("\$inc" to mapOf("visits" to 1L)))
                }
            }

            threads.forEach { it.start() }
            threads.forEach { it.join() }

            // Verify null metadata survived concurrent updates
            val result = storage.findById("id1")!!
            assertThat(result.get("metadata").isNull).isTrue()
            assertThat(result.get("visits").asLong()).isEqualTo(10L)
        }
    }

    // Helper function to create test documents
    private fun createDocument(id: String, name: String, age: Int, visits: Long): ObjectNode {
        val doc = mapper.createObjectNode()
        doc.put("_id", id)
        doc.put("name", name)
        doc.put("age", age)
        doc.put("email", "${name.lowercase().replace(" ", "")}@example.com")
        doc.put("visits", visits)
        return doc
    }
}
