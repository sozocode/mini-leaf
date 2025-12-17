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
 * Comprehensive unit test for MongoServerMetrics temporal queries.
 * Tests the exact user scenario with cnxId + timestamp range filtering.
 */
class MongoServerMetricsQueryTest {

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
    fun `query by cnxId only returns correct documents`() {
        val repository = db.getRepository<MongoServerMetrics, ObjectId>(
            "server_metrics",
            JacksonCodec(MongoServerMetrics::class.java)
        )

        // Create test data with different cnxIds
        val now = Instant.now()
        val metrics1 = createMetrics("connection-1", now)
        val metrics2 = createMetrics("connection-1", now.plusSeconds(10))
        val metrics3 = createMetrics("connection-2", now.plusSeconds(20))
        val metrics4 = createMetrics("connection-1", now.plusSeconds(30))

        repository.saveAll(listOf(metrics1, metrics2, metrics3, metrics4))

        // Query by cnxId only
        val results = repository.findAll(
            filter = mapOf("cnxId" to "connection-1"),
            skip = 0,
            limit = Int.MAX_VALUE
        )

        assertThat(results).hasSize(3)
        assertThat(results).allMatch { it.cnxId == "connection-1" }
    }

    @Test
    fun `query by timestamp range only returns correct documents`() {
        val repository = db.getRepository<MongoServerMetrics, ObjectId>(
            "server_metrics",
            JacksonCodec(MongoServerMetrics::class.java)
        )

        // Create test data with different timestamps
        val baseTime = Instant.parse("2024-01-01T12:00:00Z")
        val metrics1 = createMetrics("conn-1", baseTime.minusSeconds(7200)) // 2 hours before
        val metrics2 = createMetrics("conn-1", baseTime.minusSeconds(1800)) // 30 min before
        val metrics3 = createMetrics("conn-1", baseTime) // exact time
        val metrics4 = createMetrics("conn-1", baseTime.plusSeconds(1800)) // 30 min after
        val metrics5 = createMetrics("conn-1", baseTime.plusSeconds(7200)) // 2 hours after

        repository.saveAll(listOf(metrics1, metrics2, metrics3, metrics4, metrics5))

        // Query for 1 hour window around baseTime
        val startTime = baseTime.minusSeconds(3600)
        val endTime = baseTime.plusSeconds(3600)

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

        assertThat(results).hasSize(3) // metrics2, metrics3, metrics4
        assertThat(results).allMatch {
            it.timestamp >= startTime && it.timestamp <= endTime
        }
    }

    @Test
    fun `query by both cnxId and timestamp range returns correct documents`() {
        val repository = db.getRepository<MongoServerMetrics, ObjectId>(
            "server_metrics",
            JacksonCodec(MongoServerMetrics::class.java)
        )

        // Create test data with mixed cnxIds and timestamps
        val baseTime = Instant.parse("2024-01-01T12:00:00Z")

        // connection-1 metrics
        val conn1_old = createMetrics("connection-1", baseTime.minusSeconds(7200))
        val conn1_recent1 = createMetrics("connection-1", baseTime.minusSeconds(600)) // 10 min ago
        val conn1_recent2 = createMetrics("connection-1", baseTime)
        val conn1_recent3 = createMetrics("connection-1", baseTime.plusSeconds(600)) // 10 min later
        val conn1_future = createMetrics("connection-1", baseTime.plusSeconds(7200))

        // connection-2 metrics (should be filtered out)
        val conn2_recent1 = createMetrics("connection-2", baseTime.minusSeconds(600))
        val conn2_recent2 = createMetrics("connection-2", baseTime)

        repository.saveAll(listOf(
            conn1_old, conn1_recent1, conn1_recent2, conn1_recent3, conn1_future,
            conn2_recent1, conn2_recent2
        ))

        // Query for connection-1 in the last hour
        val startTime = baseTime.minusSeconds(3600)
        val endTime = baseTime.plusSeconds(3600)

        val results = repository.findAll(
            filter = mapOf(
                "cnxId" to "connection-1",
                "timestamp" to mapOf(
                    "\$gte" to startTime,
                    "\$lte" to endTime
                )
            ),
            skip = 0,
            limit = Int.MAX_VALUE
        ).sortedBy { it.timestamp }

        // Should get only connection-1 metrics within time range
        assertThat(results).hasSize(3)
        assertThat(results).allMatch { it.cnxId == "connection-1" }
        assertThat(results).allMatch {
            it.timestamp >= startTime && it.timestamp <= endTime
        }
        assertThat(results.map { it.id }).containsExactly(
            conn1_recent1.id,
            conn1_recent2.id,
            conn1_recent3.id
        )
    }

    @Test
    fun `exact user scenario - findByMetadataAndTimeRange`() {
        val repository = db.getRepository<MongoServerMetrics, ObjectId>(
            "mongoMetrics",
            JacksonCodec(MongoServerMetrics::class.java)
        )

        // User's exact method
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

        // Create realistic test data
        val now = Instant.now()
        val testData = mutableListOf<MongoServerMetrics>()

        // Generate 100 metrics over last 10 minutes for connection-1
        for (i in 0 until 100) {
            testData.add(
                createMetrics(
                    cnxId = "connection-1",
                    timestamp = now.minusSeconds(600L - i * 6) // Every 6 seconds
                )
            )
        }

        // Add some metrics for connection-2 (should be filtered out)
        for (i in 0 until 20) {
            testData.add(
                createMetrics(
                    cnxId = "connection-2",
                    timestamp = now.minusSeconds(300L - i * 15)
                )
            )
        }

        // Add old metrics for connection-1 (should be filtered out by time)
        for (i in 0 until 10) {
            testData.add(
                createMetrics(
                    cnxId = "connection-1",
                    timestamp = now.minusSeconds(7200L + i * 60) // From 2 hours ago
                )
            )
        }

        repository.saveAll(testData)

        // Query for connection-1 in last 5 minutes
        val fiveMinutesAgo = now.minus(5, ChronoUnit.MINUTES)
        val results = findByMetadataAndTimeRange("connection-1", fiveMinutesAgo, now)

        // Should get metrics from last 5 minutes only
        val expectedCount = testData.count {
            it.cnxId == "connection-1" &&
            it.timestamp >= fiveMinutesAgo &&
            it.timestamp <= now
        }

        assertThat(results).hasSize(expectedCount)
        assertThat(results).allMatch { it.cnxId == "connection-1" }
        assertThat(results).allMatch {
            it.timestamp >= fiveMinutesAgo && it.timestamp <= now
        }
        assertThat(results).isSortedAccordingTo(compareBy { it.timestamp })
    }

    @Test
    fun `boundary conditions - exact timestamp matches`() {
        val repository = db.getRepository<MongoServerMetrics, ObjectId>(
            "server_metrics",
            JacksonCodec(MongoServerMetrics::class.java)
        )

        val exactTime = Instant.parse("2024-01-01T12:00:00Z")
        val metric = createMetrics("test-conn", exactTime)
        repository.save(metric)

        // Test $gte with exact time (should match)
        val results1 = repository.findAll(
            filter = mapOf(
                "cnxId" to "test-conn",
                "timestamp" to mapOf("\$gte" to exactTime)
            ),
            skip = 0,
            limit = 100
        )
        assertThat(results1).hasSize(1)

        // Test $lte with exact time (should match)
        val results2 = repository.findAll(
            filter = mapOf(
                "cnxId" to "test-conn",
                "timestamp" to mapOf("\$lte" to exactTime)
            ),
            skip = 0,
            limit = 100
        )
        assertThat(results2).hasSize(1)

        // Test $gt with exact time (should NOT match)
        val results3 = repository.findAll(
            filter = mapOf(
                "cnxId" to "test-conn",
                "timestamp" to mapOf("\$gt" to exactTime)
            ),
            skip = 0,
            limit = 100
        )
        assertThat(results3).isEmpty()

        // Test $lt with exact time (should NOT match)
        val results4 = repository.findAll(
            filter = mapOf(
                "cnxId" to "test-conn",
                "timestamp" to mapOf("\$lt" to exactTime)
            ),
            skip = 0,
            limit = 100
        )
        assertThat(results4).isEmpty()
    }

    @Test
    fun `empty result when no documents match criteria`() {
        val repository = db.getRepository<MongoServerMetrics, ObjectId>(
            "server_metrics",
            JacksonCodec(MongoServerMetrics::class.java)
        )

        val now = Instant.now()
        val metric = createMetrics("connection-1", now)
        repository.save(metric)

        // Query with non-existent cnxId
        val results1 = repository.findAll(
            filter = mapOf(
                "cnxId" to "non-existent",
                "timestamp" to mapOf(
                    "\$gte" to now.minusSeconds(60),
                    "\$lte" to now.plusSeconds(60)
                )
            ),
            skip = 0,
            limit = Int.MAX_VALUE
        )
        assertThat(results1).isEmpty()

        // Query with time range that doesn't include the document
        val results2 = repository.findAll(
            filter = mapOf(
                "cnxId" to "connection-1",
                "timestamp" to mapOf(
                    "\$gte" to now.plusSeconds(100),
                    "\$lte" to now.plusSeconds(200)
                )
            ),
            skip = 0,
            limit = Int.MAX_VALUE
        )
        assertThat(results2).isEmpty()
    }

    @Test
    fun `large dataset performance test`() {
        val repository = db.getRepository<MongoServerMetrics, ObjectId>(
            "server_metrics",
            JacksonCodec(MongoServerMetrics::class.java)
        )

        // Create 1000 metrics across 10 connections over 1 hour
        val now = Instant.now()
        val metrics = mutableListOf<MongoServerMetrics>()

        for (i in 0 until 1000) {
            metrics.add(
                createMetrics(
                    cnxId = "connection-${i % 10}",
                    timestamp = now.minusSeconds(3600L - i * 3) // Every 3 seconds
                )
            )
        }

        repository.saveAll(metrics)

        // Query for one specific connection in last 30 minutes
        val thirtyMinutesAgo = now.minus(30, ChronoUnit.MINUTES)
        val results = repository.findAll(
            filter = mapOf(
                "cnxId" to "connection-5",
                "timestamp" to mapOf(
                    "\$gte" to thirtyMinutesAgo,
                    "\$lte" to now
                )
            ),
            skip = 0,
            limit = Int.MAX_VALUE
        )

        // Verify we got the right subset
        val expected = metrics.count {
            it.cnxId == "connection-5" &&
            it.timestamp >= thirtyMinutesAgo &&
            it.timestamp <= now
        }

        assertThat(results).hasSize(expected)
        assertThat(results).allMatch { it.cnxId == "connection-5" }
    }

    @Test
    fun `nanosecond precision timestamps work correctly`() {
        val repository = db.getRepository<MongoServerMetrics, ObjectId>(
            "server_metrics",
            JacksonCodec(MongoServerMetrics::class.java)
        )

        // Create timestamps with nanosecond precision
        val base = Instant.parse("2024-01-01T12:00:00Z")
        val time1 = base.plusNanos(123456789)
        val time2 = base.plusNanos(987654321)
        val time3 = base.plusSeconds(1).plusNanos(111111111)

        val metric1 = createMetrics("conn-1", time1)
        val metric2 = createMetrics("conn-1", time2)
        val metric3 = createMetrics("conn-1", time3)

        repository.saveAll(listOf(metric1, metric2, metric3))

        // Query with precise time range
        val results = repository.findAll(
            filter = mapOf(
                "cnxId" to "conn-1",
                "timestamp" to mapOf(
                    "\$gte" to base,
                    "\$lt" to base.plusSeconds(1)
                )
            ),
            skip = 0,
            limit = Int.MAX_VALUE
        )

        // Should get only the first 2 metrics (time3 is after 1 second)
        assertThat(results).hasSize(2)
    }

    // Helper function to create test metrics
    private fun createMetrics(
        cnxId: String,
        timestamp: Instant,
        connections: Int = 10
    ): MongoServerMetrics {
        return MongoServerMetrics(
            id = ObjectId(),
            cnxId = cnxId,
            db = "testdb",
            timestamp = timestamp,
            currentConnections = connections,
            activeConnections = connections / 2,
            availableConnections = connections / 2,
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
    }
}
