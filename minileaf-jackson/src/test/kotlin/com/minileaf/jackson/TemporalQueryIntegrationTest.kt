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
import java.time.temporal.ChronoUnit

/**
 * Integration test to verify temporal type queries work end-to-end.
 * Tests the exact scenario from MongoServerMetrics.
 */
class TemporalQueryIntegrationTest {

    @TempDir
    lateinit var tempDir: Path

    private lateinit var db: Minileaf

    data class MongoServerMetrics(
        val id: ObjectId = ObjectId(),
        val cnxId: String,
        val db: String,
        val timestamp: Instant = Instant.now(),

        // Connections
        val currentConnections: Int,
        val activeConnections: Int,
        val availableConnections: Int,
        val totalConnectionsCreated: Long,

        // Network
        val bytesIn: Long,
        val bytesOut: Long,
        val numRequests: Long,

        // Operation counts
        val insert: Long,
        val query: Long,
        val update: Long,
        val delete: Long,
        val getmore: Long,
        val command: Long,

        // Clients
        val activeReaders: Int,
        val activeWriters: Int,

        // Queue
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
    fun `temporal range query works with Instant fields`() {
        val repository = db.getRepository<MongoServerMetrics, ObjectId>(
            "metrics",
            JacksonCodec(MongoServerMetrics::class.java)
        )

        // Create test data with different timestamps
        val baseTime = Instant.parse("2024-01-01T12:00:00Z")

        val metrics1 = MongoServerMetrics(
            cnxId = "conn-1",
            db = "testdb",
            timestamp = baseTime.minusSeconds(3600), // 1 hour before
            currentConnections = 10,
            activeConnections = 5,
            availableConnections = 5,
            totalConnectionsCreated = 100,
            bytesIn = 1000,
            bytesOut = 2000,
            numRequests = 50,
            insert = 10,
            query = 20,
            update = 15,
            delete = 5,
            getmore = 0,
            command = 0,
            activeReaders = 2,
            activeWriters = 3,
            queuedReaders = 0,
            queuedWriters = 0
        )

        val metrics2 = MongoServerMetrics(
            cnxId = "conn-1",
            db = "testdb",
            timestamp = baseTime, // Exact base time
            currentConnections = 15,
            activeConnections = 8,
            availableConnections = 7,
            totalConnectionsCreated = 150,
            bytesIn = 1500,
            bytesOut = 2500,
            numRequests = 75,
            insert = 15,
            query = 30,
            update = 20,
            delete = 10,
            getmore = 0,
            command = 0,
            activeReaders = 3,
            activeWriters = 5,
            queuedReaders = 1,
            queuedWriters = 0
        )

        val metrics3 = MongoServerMetrics(
            cnxId = "conn-1",
            db = "testdb",
            timestamp = baseTime.plusSeconds(3600), // 1 hour after
            currentConnections = 20,
            activeConnections = 10,
            availableConnections = 10,
            totalConnectionsCreated = 200,
            bytesIn = 2000,
            bytesOut = 3000,
            numRequests = 100,
            insert = 20,
            query = 40,
            update = 25,
            delete = 15,
            getmore = 0,
            command = 0,
            activeReaders = 4,
            activeWriters = 6,
            queuedReaders = 0,
            queuedWriters = 1
        )

        val metrics4 = MongoServerMetrics(
            cnxId = "conn-2", // Different connection
            db = "testdb",
            timestamp = baseTime,
            currentConnections = 5,
            activeConnections = 2,
            availableConnections = 3,
            totalConnectionsCreated = 50,
            bytesIn = 500,
            bytesOut = 1000,
            numRequests = 25,
            insert = 5,
            query = 10,
            update = 7,
            delete = 3,
            getmore = 0,
            command = 0,
            activeReaders = 1,
            activeWriters = 1,
            queuedReaders = 0,
            queuedWriters = 0
        )

        // Save all metrics
        repository.save(metrics1)
        repository.save(metrics2)
        repository.save(metrics3)
        repository.save(metrics4)

        // Query with time range - THIS IS THE EXACT CODE FROM THE USER
        val startTime = baseTime.minusSeconds(1800) // 30 minutes before base
        val endTime = baseTime.plusSeconds(1800)    // 30 minutes after base

        val results = repository.findAll(
            filter = mapOf(
                "cnxId" to "conn-1",
                "timestamp" to mapOf(
                    "\$gte" to startTime,
                    "\$lte" to endTime
                )
            ),
            skip = 0,
            limit = Int.MAX_VALUE
        ).sortedBy { it.timestamp }

        // Verify results
        assertThat(results).hasSize(1)
        assertThat(results[0].timestamp).isEqualTo(baseTime)
        assertThat(results[0].cnxId).isEqualTo("conn-1")
        assertThat(results[0].currentConnections).isEqualTo(15)
    }

    @Test
    fun `exact reproduction of user scenario`() {
        val repository = db.getRepository<MongoServerMetrics, ObjectId>(
            "server_metrics",
            JacksonCodec(MongoServerMetrics::class.java)
        )

        // Insert sample data
        val now = Instant.now()
        val metrics = List(10) { i ->
            MongoServerMetrics(
                cnxId = "connection-1",
                db = "mydb",
                timestamp = now.minusSeconds(i * 60L), // Every minute for last 10 minutes
                currentConnections = 10 + i,
                activeConnections = 5 + i,
                availableConnections = 5,
                totalConnectionsCreated = 100L + i,
                bytesIn = 1000L * i,
                bytesOut = 2000L * i,
                numRequests = 50L + i,
                insert = 10L + i,
                query = 20L + i,
                update = 15L + i,
                delete = 5L + i,
                getmore = 0,
                command = 0,
                activeReaders = 2,
                activeWriters = 3,
                queuedReaders = 0,
                queuedWriters = 0
            )
        }

        metrics.forEach { repository.save(it) }

        // THE EXACT METHOD FROM THE USER'S CODE
        fun findByMetadataAndTimeRange(
            cnxId: String,
            startTime: Instant,
            endTime: Instant
        ): List<MongoServerMetrics> {
            return repository.findAll(
                filter = mapOf(
                    "cnxId" to cnxId,
                    "timestamp" to mapOf(
                        "\$gte" to startTime,
                        "\$lte" to endTime
                    )
                ),
                skip = 0,
                limit = Int.MAX_VALUE
            ).sortedBy { it.timestamp }
        }

        // Test: Last 5 minutes
        val fiveMinutesAgo = now.minus(5, ChronoUnit.MINUTES)
        val results = findByMetadataAndTimeRange("connection-1", fiveMinutesAgo, now)

        // Should get metrics from last 5 minutes (indices 0-5)
        assertThat(results).hasSizeGreaterThanOrEqualTo(5)
        assertThat(results).allMatch { it.cnxId == "connection-1" }
        assertThat(results).allMatch { it.timestamp >= fiveMinutesAgo && it.timestamp <= now }

        println("✅ Successfully queried ${results.size} metrics with temporal range")
        results.forEach {
            println("  - Timestamp: ${it.timestamp}, Connections: ${it.currentConnections}")
        }
    }

    @Test
    fun `boundary conditions work correctly`() {
        val repository = db.getRepository<MongoServerMetrics, ObjectId>(
            "metrics",
            JacksonCodec(MongoServerMetrics::class.java)
        )

        val exactTime = Instant.parse("2024-01-01T12:00:00Z")

        val metric = MongoServerMetrics(
            cnxId = "test",
            db = "db",
            timestamp = exactTime,
            currentConnections = 10,
            activeConnections = 5,
            availableConnections = 5,
            totalConnectionsCreated = 100,
            bytesIn = 1000,
            bytesOut = 2000,
            numRequests = 50,
            insert = 10,
            query = 20,
            update = 15,
            delete = 5,
            getmore = 0,
            command = 0,
            activeReaders = 2,
            activeWriters = 3,
            queuedReaders = 0,
            queuedWriters = 0
        )

        repository.save(metric)

        // Test $gte with exact time (should match)
        val results1 = repository.findAll(
            filter = mapOf("timestamp" to mapOf("\$gte" to exactTime)),
            skip = 0,
            limit = 100
        )
        assertThat(results1).hasSize(1)

        // Test $lte with exact time (should match)
        val results2 = repository.findAll(
            filter = mapOf("timestamp" to mapOf("\$lte" to exactTime)),
            skip = 0,
            limit = 100
        )
        assertThat(results2).hasSize(1)

        // Test $gt with exact time (should NOT match)
        val results3 = repository.findAll(
            filter = mapOf("timestamp" to mapOf("\$gt" to exactTime)),
            skip = 0,
            limit = 100
        )
        assertThat(results3).isEmpty()

        // Test $lt with exact time (should NOT match)
        val results4 = repository.findAll(
            filter = mapOf("timestamp" to mapOf("\$lt" to exactTime)),
            skip = 0,
            limit = 100
        )
        assertThat(results4).isEmpty()
    }
}
