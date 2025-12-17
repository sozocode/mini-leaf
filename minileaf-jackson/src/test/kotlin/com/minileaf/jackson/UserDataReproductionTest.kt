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
 * Test with EXACT user data to reproduce the issue.
 */
class UserDataReproductionTest {

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
    fun `exact user scenario with actual data`() {
        val repository = db.getRepository<MongoServerMetrics, ObjectId>(
            "mongoMetrics",
            JacksonCodec(MongoServerMetrics::class.java)
        )

        // Create document with EXACT values from user's database
        val doc = MongoServerMetrics(
            id = ObjectId("68f9c438b7d005388339fc3e"),
            cnxId = "f2ca4a47-f027-4e43-8bf6-fd8cfe02fb77",
            timestamp = Instant.ofEpochSecond(1761199160, 443513000), // 1761199160.443513 seconds
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

        println("=== SAVING DOCUMENT ===")
        println("cnxId: ${doc.cnxId}")
        println("timestamp: ${doc.timestamp}")
        println("timestamp epoch seconds: ${doc.timestamp.epochSecond}.${doc.timestamp.nano}")

        repository.save(doc)

        // Verify it was saved
        val all = repository.findAll()
        println("\n=== ALL DOCUMENTS ===")
        println("Total: ${all.size}")
        all.forEach {
            println("  cnxId: ${it.cnxId}, timestamp: ${it.timestamp}")
        }

        // USER'S EXACT QUERY
        val cnxId = "f2ca4a47-f027-4e43-8bf6-fd8cfe02fb77"
        val startTime = Instant.parse("1970-01-01T00:00:00Z")
        val endTime = Instant.parse("2025-10-23T05:59:54.263390Z")

        println("\n=== QUERY PARAMETERS ===")
        println("cnxId: $cnxId")
        println("startTime: $startTime (epoch ${startTime.toEpochMilli()} ms)")
        println("endTime: $endTime (epoch ${endTime.toEpochMilli()} ms)")
        println("Document timestamp: ${doc.timestamp} (epoch ${doc.timestamp.toEpochMilli()} ms)")

        // Check if document should match
        val shouldMatch = doc.cnxId == cnxId &&
                         doc.timestamp >= startTime &&
                         doc.timestamp <= endTime
        println("\nShould match: $shouldMatch")
        println("  cnxId matches: ${doc.cnxId == cnxId}")
        println("  >= startTime: ${doc.timestamp >= startTime} (${doc.timestamp} >= $startTime)")
        println("  <= endTime: ${doc.timestamp <= endTime} (${doc.timestamp} <= $endTime)")

        println("\n=== EXECUTING QUERY ===")
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
        results.forEach {
            println("  Found: cnxId=${it.cnxId}, timestamp=${it.timestamp}")
        }

        // This should return 1 document
        assertThat(results).hasSize(1)
        assertThat(results[0].cnxId).isEqualTo(cnxId)
    }

    @Test
    fun `query with only cnxId filter`() {
        val repository = db.getRepository<MongoServerMetrics, ObjectId>(
            "mongoMetrics",
            JacksonCodec(MongoServerMetrics::class.java)
        )

        val doc = MongoServerMetrics(
            id = ObjectId("68f9c438b7d005388339fc3e"),
            cnxId = "f2ca4a47-f027-4e43-8bf6-fd8cfe02fb77",
            timestamp = Instant.ofEpochSecond(1761199160, 443513000),
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

        repository.save(doc)

        // Test cnxId filter ONLY
        val cnxId = "f2ca4a47-f027-4e43-8bf6-fd8cfe02fb77"

        println("=== QUERY BY cnxId ONLY ===")
        val results = repository.findAll(
            filter = mapOf("cnxId" to cnxId),
            skip = 0,
            limit = Int.MAX_VALUE
        )

        println("Results: ${results.size}")

        assertThat(results).hasSize(1)
        assertThat(results[0].cnxId).isEqualTo(cnxId)
    }

    @Test
    fun `query with only timestamp filter`() {
        val repository = db.getRepository<MongoServerMetrics, ObjectId>(
            "mongoMetrics",
            JacksonCodec(MongoServerMetrics::class.java)
        )

        val doc = MongoServerMetrics(
            id = ObjectId("68f9c438b7d005388339fc3e"),
            cnxId = "f2ca4a47-f027-4e43-8bf6-fd8cfe02fb77",
            timestamp = Instant.ofEpochSecond(1761199160, 443513000),
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

        repository.save(doc)

        // Test timestamp filter ONLY
        val startTime = Instant.parse("1970-01-01T00:00:00Z")
        val endTime = Instant.parse("2025-10-23T05:59:54.263390Z")

        println("=== QUERY BY timestamp ONLY ===")
        println("Range: $startTime to $endTime")
        println("Document: ${doc.timestamp}")

        val results = repository.findAll(
            filter = mapOf(
                "timestamp" to mapOf(
                    "\$gte" to startTime,
                    "\$lte" to endTime
                )
            ),
            skip = 0,
            limit = Int.MAX_VALUE
        )

        println("Results: ${results.size}")

        assertThat(results).hasSize(1)
    }
}
