package com.minileaf.jackson

import com.minileaf.core.Minileaf
import com.minileaf.core.config.MinileafConfig
import org.bson.types.ObjectId
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import java.time.Instant

class DebugUserScenario {

    @TempDir
    lateinit var tempDir: Path

    private lateinit var db: Minileaf

    data class MongoServerMetrics(
        val id: ObjectId = ObjectId(),
        val cnxId: String,
        val db: String,
        val timestamp: Instant = Instant.now(),
        val currentConnections: Int
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
    fun `debug user scenario step by step`() {
        val repository = db.getRepository<MongoServerMetrics, ObjectId>(
            "server_metrics",
            JacksonCodec(MongoServerMetrics::class.java)
        )

        // Create test data
        val now = Instant.now()
        val metric1 = MongoServerMetrics(
            cnxId = "connection-1",
            db = "mydb",
            timestamp = now,
            currentConnections = 10
        )

        println("=== SAVING DOCUMENT ===")
        repository.save(metric1)
        println("Saved: cnxId=${metric1.cnxId}, timestamp=${metric1.timestamp}, connections=${metric1.currentConnections}")

        // Step 1: Get all documents
        println("\n=== STEP 1: findAll() with NO filter ===")
        val allDocs = repository.findAll()
        println("Total documents: ${allDocs.size}")
        allDocs.forEach { doc ->
            println("  Doc: cnxId=${doc.cnxId}, timestamp=${doc.timestamp}, connections=${doc.currentConnections}")
        }

        // Step 2: Filter by cnxId only
        println("\n=== STEP 2: Filter by cnxId ONLY ===")
        val byCnxId = repository.findAll(
            filter = mapOf("cnxId" to "connection-1"),
            skip = 0,
            limit = Int.MAX_VALUE
        )
        println("Results by cnxId: ${byCnxId.size}")
        byCnxId.forEach { doc ->
            println("  Doc: cnxId=${doc.cnxId}, timestamp=${doc.timestamp}")
        }

        // Step 3: Filter by timestamp only
        println("\n=== STEP 3: Filter by timestamp ONLY ===")
        val startTime = now.minusSeconds(60)
        val endTime = now.plusSeconds(60)
        println("Query range: $startTime to $endTime")
        println("Document timestamp: ${metric1.timestamp}")
        println("In range: ${metric1.timestamp >= startTime && metric1.timestamp <= endTime}")

        val byTimestamp = repository.findAll(
            filter = mapOf(
                "timestamp" to mapOf(
                    "\$gte" to startTime,
                    "\$lte" to endTime
                )
            ),
            skip = 0,
            limit = Int.MAX_VALUE
        )
        println("Results by timestamp: ${byTimestamp.size}")
        byTimestamp.forEach { doc ->
            println("  Doc: cnxId=${doc.cnxId}, timestamp=${doc.timestamp}")
        }

        // Step 4: Filter by BOTH cnxId and timestamp
        println("\n=== STEP 4: Filter by BOTH cnxId AND timestamp ===")
        val byBoth = repository.findAll(
            filter = mapOf(
                "cnxId" to "connection-1",
                "timestamp" to mapOf(
                    "\$gte" to startTime,
                    "\$lte" to endTime
                )
            ),
            skip = 0,
            limit = Int.MAX_VALUE
        )
        println("Results by BOTH: ${byBoth.size}")
        byBoth.forEach { doc ->
            println("  Doc: cnxId=${doc.cnxId}, timestamp=${doc.timestamp}")
        }

        // Step 5: Check raw ObjectNode structure
        println("\n=== STEP 5: Check RAW document structure ===")
        val codec = JacksonCodec(MongoServerMetrics::class.java)
        if (allDocs.isNotEmpty()) {
            val rawNode = codec.toNode(allDocs[0])
            println("Raw ObjectNode: $rawNode")
            println("Field names: ${rawNode.fieldNames().asSequence().toList()}")
            rawNode.fields().forEach { (name, value) ->
                println("  $name: $value (type: ${value.javaClass.simpleName}, isTextual: ${value.isTextual}, isNumber: ${value.isNumber})")
            }
        }

        println("\n=== SUMMARY ===")
        println("All docs: ${allDocs.size}")
        println("By cnxId: ${byCnxId.size}")
        println("By timestamp: ${byTimestamp.size}")
        println("By BOTH: ${byBoth.size}")
    }
}
