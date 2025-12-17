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
import java.util.*

/**
 * Test to verify that Date/Instant values are serialized consistently
 * when using partial updates ($set) vs full entity saves.
 */
class DatePartialUpdateTest {

    @TempDir
    lateinit var tempDir: Path

    private lateinit var db: Minileaf

    data class EventLog(
        val id: ObjectId = ObjectId(),
        val name: String,
        val createdAt: Instant,
        val updatedAt: Instant,
        val processedAt: Date? = null
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
    fun `Date field saved via partial update should match full save serialization`() {
        val repository = db.getRepository<EventLog, ObjectId>(
            "events",
            JacksonCodec(EventLog::class.java)
        )

        val now = Instant.now()
        val processedDate = Date()

        // Create initial event without processedAt
        val event = EventLog(
            name = "Test Event",
            createdAt = now,
            updatedAt = now,
            processedAt = null
        )
        val saved = repository.save(event)

        println("=== Initial Event ===")
        println("Event: $saved")
        println("ProcessedAt: ${saved.processedAt}")

        // Update processedAt using partial update
        val updated = repository.updateById(
            saved.id,
            mapOf(
                "\$set" to mapOf(
                    "processedAt" to processedDate
                )
            )
        )

        assertThat(updated).isTrue()

        // Retrieve and verify
        val resultAfterPartialUpdate = repository.findById(saved.id).get()
        println("\n=== After Partial Update ===")
        println("Event: $resultAfterPartialUpdate")
        println("ProcessedAt: ${resultAfterPartialUpdate.processedAt}")

        // Now create a new event with processedAt set directly
        val event2 = EventLog(
            name = "Test Event 2",
            createdAt = now,
            updatedAt = now,
            processedAt = processedDate
        )
        val saved2 = repository.save(event2)

        println("\n=== Event Saved Directly with Date ===")
        println("Event: $saved2")
        println("ProcessedAt: ${saved2.processedAt}")

        // Both should serialize the same way
        assertThat(resultAfterPartialUpdate.processedAt).isNotNull
        assertThat(saved2.processedAt).isNotNull

        // The dates should be equal (allowing for millisecond precision)
        assertThat(resultAfterPartialUpdate.processedAt?.time).isEqualTo(processedDate.time)
        assertThat(saved2.processedAt?.time).isEqualTo(processedDate.time)
    }

    @Test
    fun `Instant field updated via partial update should match full save serialization`() {
        val repository = db.getRepository<EventLog, ObjectId>(
            "events",
            JacksonCodec(EventLog::class.java)
        )

        val now = Instant.now()
        val newUpdateTime = Instant.parse("2024-01-15T10:30:00Z")

        // Create initial event
        val event = EventLog(
            name = "Test Event",
            createdAt = now,
            updatedAt = now
        )
        val saved = repository.save(event)

        println("=== Initial Event ===")
        println("UpdatedAt: ${saved.updatedAt}")

        // Update updatedAt using partial update
        repository.updateById(
            saved.id,
            mapOf(
                "\$set" to mapOf(
                    "updatedAt" to newUpdateTime
                )
            )
        )

        // Retrieve and verify
        val resultAfterPartialUpdate = repository.findById(saved.id).get()
        println("\n=== After Partial Update ===")
        println("UpdatedAt: ${resultAfterPartialUpdate.updatedAt}")

        // Now create a new event with the same updatedAt
        val event2 = EventLog(
            name = "Test Event 2",
            createdAt = now,
            updatedAt = newUpdateTime
        )
        val saved2 = repository.save(event2)

        println("\n=== Event Saved Directly with Instant ===")
        println("UpdatedAt: ${saved2.updatedAt}")

        // Both should serialize the same way
        assertThat(resultAfterPartialUpdate.updatedAt).isEqualTo(newUpdateTime)
        assertThat(saved2.updatedAt).isEqualTo(newUpdateTime)
    }

    @Test
    fun `partial update persists across database reopens`() {
        val repository = db.getRepository<EventLog, ObjectId>(
            "events",
            JacksonCodec(EventLog::class.java)
        )

        val now = Instant.now()
        val processedDate = Date()

        // Create and update event
        val event = EventLog(
            name = "Persistent Event",
            createdAt = now,
            updatedAt = now
        )
        val saved = repository.save(event)

        repository.updateById(
            saved.id,
            mapOf(
                "\$set" to mapOf(
                    "processedAt" to processedDate
                )
            )
        )

        // Close and reopen database
        db.close()

        val config = MinileafConfig(
            dataDir = tempDir,
            memoryOnly = false
        )
        db = Minileaf.open(config)

        val newRepository = db.getRepository<EventLog, ObjectId>(
            "events",
            JacksonCodec(EventLog::class.java)
        )

        // Verify date persisted correctly
        val result = newRepository.findById(saved.id).get()
        assertThat(result.processedAt).isNotNull
        assertThat(result.processedAt?.time).isEqualTo(processedDate.time)
    }
}
