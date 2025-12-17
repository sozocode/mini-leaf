package com.minileaf.jackson

import com.minileaf.core.Minileaf
import com.minileaf.core.config.MinileafConfig
import org.assertj.core.api.Assertions.assertThat
import org.bson.types.ObjectId
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import java.time.Instant

/**
 * Test that reproduces the EXACT user scenario:
 * 1. Delete all documents
 * 2. Save a new document
 * 3. Immediately query for it
 *
 * This should work but user reports it returns 0 results.
 */
class FreshDocumentQueryTest {

    @TempDir
    lateinit var tempDir: Path

    private lateinit var db: Minileaf

    data class MongoServerMetrics(
        val id: ObjectId = ObjectId(),
        val cnxId: String,
        val timestamp: Instant,
        val currentConnections: Int,
        val activeConnections: Int,
        val availableConnections: Int,
        val totalConnectionsCreated: Long,
        val bytesIn: Long,
        val bytesOut: Long,
        val numRequests: Long,
        val insert: Long,
        val query: Long,
        val update: Long,
        val delete: Long,
        val getmore: Long,
        val command: Long,
        val activeReaders: Int,
        val activeWriters: Int,
        val queuedReaders: Int,
        val queuedWriters: Int
    )

    @BeforeEach
    fun setup() {
        val config = MinileafConfig(
            dataDir = tempDir,
            memoryOnly = false
        )
        db = Minileaf.open(config)
    }

    @AfterEach
    fun cleanup() {
        db.close()
    }

    @Test
    fun `USER SCENARIO - delete all, record, query immediately`() {
        val repository = db.getRepository<MongoServerMetrics, ObjectId>(
            "mongoMetrics",
            JacksonCodec(MongoServerMetrics::class.java)
        )

        val cnxId = "f2ca4a47-f027-4e43-8bf6-fd8cfe02fb77"

        println("=== STEP 1: Delete all documents for cnxId ===")
        val existing = repository.findAll(
            filter = mapOf("cnxId" to cnxId),
            skip = 0,
            limit = Int.MAX_VALUE
        )
        println("Found ${existing.size} existing documents")
        existing.forEach { repository.deleteById(it.id) }
        println("Deleted all documents")

        println("\n=== STEP 2: Save a new document (RECORDING) ===")
        val now = Instant.now()
        val newDoc = MongoServerMetrics(
            id = ObjectId(),
            cnxId = cnxId,
            timestamp = now,
            currentConnections = 10,
            activeConnections = 2,
            availableConnections = 838786,
            totalConnectionsCreated = 0,
            bytesIn = 1000,
            bytesOut = 85000,
            numRequests = 5,
            insert = 0,
            query = 0,
            update = 0,
            delete = 0,
            getmore = 0,
            command = 5,
            activeReaders = 0,
            activeWriters = 0,
            queuedReaders = 0,
            queuedWriters = 0
        )

        println("Saving document:")
        println("  cnxId: ${newDoc.cnxId}")
        println("  timestamp: ${newDoc.timestamp}")
        println("  timestamp epochMillis: ${newDoc.timestamp.toEpochMilli()}")

        val saved = repository.save(newDoc)
        println("Document saved with ID: ${saved.id}")

        println("\n=== STEP 3: Verify it was saved ===")
        val allDocs = repository.findAll()
        println("Total documents in DB: ${allDocs.size}")

        println("\n=== STEP 4: Query immediately (USER'S QUERY) ===")
        val startTime = Instant.EPOCH
        val endTime = Instant.now()

        println("Query parameters:")
        println("  cnxId: $cnxId")
        println("  startTime: $startTime")
        println("  endTime: $endTime")

        val results = repository.findAll(
            filter = mapOf(
                "cnxId" to cnxId,
                "timestamp" to mapOf(
                    "\$gte" to startTime,
                    "\$lte" to endTime
                )
            ),
            skip = 0,
            limit = Int.MAX_VALUE
        )

        println("\nResults: ${results.size}")
        if (results.isEmpty()) {
            println("❌ REPRODUCED THE BUG! Query returned 0 results!")

            // Debug: check if document matches manually
            println("\n=== MANUAL CHECK ===")
            println("Document cnxId matches: ${saved.cnxId == cnxId}")
            println("Document timestamp >= startTime: ${saved.timestamp >= startTime}")
            println("Document timestamp <= endTime: ${saved.timestamp <= endTime}")
            println("Should match: ${saved.cnxId == cnxId && saved.timestamp >= startTime && saved.timestamp <= endTime}")
        } else {
            println("✅ Query returned ${results.size} results")
            results.forEach {
                println("  Found: cnxId=${it.cnxId}, timestamp=${it.timestamp}")
            }
        }

        // This SHOULD pass
        assertThat(results).hasSize(1)
        assertThat(results[0].id).isEqualTo(saved.id)
    }

    @Test
    fun `save and query multiple times`() {
        val repository = db.getRepository<MongoServerMetrics, ObjectId>(
            "mongoMetrics",
            JacksonCodec(MongoServerMetrics::class.java)
        )

        val cnxId = "test-connection"
        val startTime = Instant.EPOCH
        val endTime = Instant.now()

        println("=== Save 5 documents ===")
        val savedDocs = mutableListOf<MongoServerMetrics>()
        for (i in 1..5) {
            val doc = MongoServerMetrics(
                id = ObjectId(),
                cnxId = cnxId,
                timestamp = Instant.now().minusSeconds(60L * i),
                currentConnections = 10,
                activeConnections = 2,
                availableConnections = 100,
                totalConnectionsCreated = 0,
                bytesIn = 1000,
                bytesOut = 2000,
                numRequests = 5,
                insert = 0,
                query = 0,
                update = 0,
                delete = 0,
                getmore = 0,
                command = 5,
                activeReaders = 0,
                activeWriters = 0,
                queuedReaders = 0,
                queuedWriters = 0
            )
            savedDocs.add(repository.save(doc))
            println("Saved doc $i: timestamp=${doc.timestamp}")
        }

        println("\n=== Query all documents ===")
        val results = repository.findAll(
            filter = mapOf(
                "cnxId" to cnxId,
                "timestamp" to mapOf(
                    "\$gte" to startTime,
                    "\$lte" to endTime
                )
            ),
            skip = 0,
            limit = Int.MAX_VALUE
        )

        println("Results: ${results.size}")
        assertThat(results).hasSize(5)
    }
}
