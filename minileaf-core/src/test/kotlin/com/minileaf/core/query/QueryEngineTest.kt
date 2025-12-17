package com.minileaf.core.query

import com.fasterxml.jackson.databind.node.ObjectNode
import com.minileaf.core.document.DocumentUtils
import org.assertj.core.api.Assertions.assertThat
import org.bson.types.ObjectId
import org.junit.jupiter.api.Test
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset

class QueryEngineTest {

    private fun createDoc(vararg pairs: Pair<String, Any>): ObjectNode {
        val doc = DocumentUtils.createDocument()
        pairs.forEach { (key, value) ->
            when (value) {
                is String -> doc.put(key, value)
                is Int -> doc.put(key, value)
                is Long -> doc.put(key, value)
                is Double -> doc.put(key, value)
                is Boolean -> doc.put(key, value)
            }
        }
        return doc
    }

    @Test
    fun `empty filter matches all documents`() {
        val doc = createDoc("name" to "Alice", "age" to 30)
        assertThat(QueryEngine.matches(doc, emptyMap())).isTrue
    }

    @Test
    fun `simple equality filter`() {
        val doc = createDoc("name" to "Alice", "age" to 30)

        assertThat(QueryEngine.matches(doc, mapOf("name" to "Alice"))).isTrue
        assertThat(QueryEngine.matches(doc, mapOf("name" to "Bob"))).isFalse
        assertThat(QueryEngine.matches(doc, mapOf("age" to 30))).isTrue
        assertThat(QueryEngine.matches(doc, mapOf("age" to 25))).isFalse
    }

    @Test
    fun `gt operator`() {
        val doc = createDoc("age" to 30)

        assertThat(QueryEngine.matches(doc, mapOf("age" to mapOf("\$gt" to 25)))).isTrue
        assertThat(QueryEngine.matches(doc, mapOf("age" to mapOf("\$gt" to 30)))).isFalse
        assertThat(QueryEngine.matches(doc, mapOf("age" to mapOf("\$gt" to 35)))).isFalse
    }

    @Test
    fun `gte operator`() {
        val doc = createDoc("age" to 30)

        assertThat(QueryEngine.matches(doc, mapOf("age" to mapOf("\$gte" to 30)))).isTrue
        assertThat(QueryEngine.matches(doc, mapOf("age" to mapOf("\$gte" to 25)))).isTrue
        assertThat(QueryEngine.matches(doc, mapOf("age" to mapOf("\$gte" to 35)))).isFalse
    }

    @Test
    fun `lt operator`() {
        val doc = createDoc("age" to 30)

        assertThat(QueryEngine.matches(doc, mapOf("age" to mapOf("\$lt" to 35)))).isTrue
        assertThat(QueryEngine.matches(doc, mapOf("age" to mapOf("\$lt" to 30)))).isFalse
        assertThat(QueryEngine.matches(doc, mapOf("age" to mapOf("\$lt" to 25)))).isFalse
    }

    @Test
    fun `lte operator`() {
        val doc = createDoc("age" to 30)

        assertThat(QueryEngine.matches(doc, mapOf("age" to mapOf("\$lte" to 30)))).isTrue
        assertThat(QueryEngine.matches(doc, mapOf("age" to mapOf("\$lte" to 35)))).isTrue
        assertThat(QueryEngine.matches(doc, mapOf("age" to mapOf("\$lte" to 25)))).isFalse
    }

    @Test
    fun `ne operator`() {
        val doc = createDoc("age" to 30)

        assertThat(QueryEngine.matches(doc, mapOf("age" to mapOf("\$ne" to 25)))).isTrue
        assertThat(QueryEngine.matches(doc, mapOf("age" to mapOf("\$ne" to 30)))).isFalse
    }

    @Test
    fun `range query`() {
        val doc = createDoc("age" to 30)

        val filter = mapOf("age" to mapOf("\$gte" to 25, "\$lte" to 35))
        assertThat(QueryEngine.matches(doc, filter)).isTrue

        val filter2 = mapOf("age" to mapOf("\$gte" to 35, "\$lte" to 40))
        assertThat(QueryEngine.matches(doc, filter2)).isFalse
    }

    @Test
    fun `in operator`() {
        val doc = createDoc("country" to "US")

        val filter = mapOf("country" to mapOf("\$in" to listOf("US", "CA", "UK")))
        assertThat(QueryEngine.matches(doc, filter)).isTrue

        val filter2 = mapOf("country" to mapOf("\$in" to listOf("FR", "DE")))
        assertThat(QueryEngine.matches(doc, filter2)).isFalse
    }

    @Test
    fun `nin operator`() {
        val doc = createDoc("country" to "US")

        val filter = mapOf("country" to mapOf("\$nin" to listOf("FR", "DE")))
        assertThat(QueryEngine.matches(doc, filter)).isTrue

        val filter2 = mapOf("country" to mapOf("\$nin" to listOf("US", "CA")))
        assertThat(QueryEngine.matches(doc, filter2)).isFalse
    }

    @Test
    fun `exists operator`() {
        val doc = createDoc("name" to "Alice", "age" to 30)

        assertThat(QueryEngine.matches(doc, mapOf("name" to mapOf("\$exists" to true)))).isTrue
        assertThat(QueryEngine.matches(doc, mapOf("email" to mapOf("\$exists" to false)))).isTrue
        assertThat(QueryEngine.matches(doc, mapOf("name" to mapOf("\$exists" to false)))).isFalse
    }

    @Test
    fun `and operator`() {
        val doc = createDoc("name" to "Alice", "age" to 30)

        val filter = mapOf(
            "\$and" to listOf(
                mapOf("name" to "Alice"),
                mapOf("age" to mapOf("\$gte" to 25))
            )
        )
        assertThat(QueryEngine.matches(doc, filter)).isTrue

        val filter2 = mapOf(
            "\$and" to listOf(
                mapOf("name" to "Alice"),
                mapOf("age" to mapOf("\$lt" to 25))
            )
        )
        assertThat(QueryEngine.matches(doc, filter2)).isFalse
    }

    @Test
    fun `or operator`() {
        val doc = createDoc("name" to "Alice", "age" to 30)

        val filter = mapOf(
            "\$or" to listOf(
                mapOf("name" to "Bob"),
                mapOf("age" to 30)
            )
        )
        assertThat(QueryEngine.matches(doc, filter)).isTrue

        val filter2 = mapOf(
            "\$or" to listOf(
                mapOf("name" to "Bob"),
                mapOf("age" to 25)
            )
        )
        assertThat(QueryEngine.matches(doc, filter2)).isFalse
    }

    @Test
    fun `regex operator with case sensitivity`() {
        val doc = createDoc("email" to "alice@example.com")

        val filter = mapOf("email" to mapOf("\$regex" to ".*@example\\.com$"))
        assertThat(QueryEngine.matches(doc, filter)).isTrue

        val filter2 = mapOf("email" to mapOf("\$regex" to ".*@test\\.com$"))
        assertThat(QueryEngine.matches(doc, filter2)).isFalse
    }

    @Test
    fun `regex operator with case insensitive`() {
        val doc = createDoc("email" to "Alice@Example.COM")

        val filter = mapOf(
            "email" to mapOf(
                "\$regex" to ".*@example\\.com$",
                "\$options" to "i"
            )
        )
        assertThat(QueryEngine.matches(doc, filter)).isTrue
    }

    @Test
    fun `complex nested query`() {
        val doc = createDoc("name" to "Alice", "age" to 30, "country" to "US")

        val filter = mapOf(
            "\$and" to listOf(
                mapOf(
                    "\$or" to listOf(
                        mapOf("country" to "US"),
                        mapOf("country" to "CA")
                    )
                ),
                mapOf("age" to mapOf("\$gte" to 25, "\$lte" to 35))
            )
        )
        assertThat(QueryEngine.matches(doc, filter)).isTrue
    }

    @Test
    fun `temporal comparison with Instant - gte and lte`() {
        // Create a document with timestamp as epoch milliseconds (as Jackson serializes it)
        val now = Instant.now()
        val doc = DocumentUtils.createDocument()
        doc.put("timestamp", now.toEpochMilli())

        // Query with Instant objects
        val startTime = now.minusSeconds(60)
        val endTime = now.plusSeconds(60)

        val filter = mapOf(
            "timestamp" to mapOf(
                "\$gte" to startTime,
                "\$lte" to endTime
            )
        )

        assertThat(QueryEngine.matches(doc, filter)).isTrue
    }

    @Test
    fun `temporal comparison with Instant - outside range`() {
        val now = Instant.now()
        val doc = DocumentUtils.createDocument()
        doc.put("timestamp", now.toEpochMilli())

        // Query with range that doesn't include now
        val startTime = now.plusSeconds(60)
        val endTime = now.plusSeconds(120)

        val filter = mapOf(
            "timestamp" to mapOf(
                "\$gte" to startTime,
                "\$lte" to endTime
            )
        )

        assertThat(QueryEngine.matches(doc, filter)).isFalse
    }

    @Test
    fun `temporal comparison with Instant - exact boundaries`() {
        val now = Instant.now()
        val doc = DocumentUtils.createDocument()
        doc.put("timestamp", now.toEpochMilli())

        // Test exact start boundary
        val filter1 = mapOf("timestamp" to mapOf("\$gte" to now))
        assertThat(QueryEngine.matches(doc, filter1)).isTrue

        // Test exact end boundary
        val filter2 = mapOf("timestamp" to mapOf("\$lte" to now))
        assertThat(QueryEngine.matches(doc, filter2)).isTrue

        // Test strict greater than
        val filter3 = mapOf("timestamp" to mapOf("\$gt" to now))
        assertThat(QueryEngine.matches(doc, filter3)).isFalse

        // Test strict less than
        val filter4 = mapOf("timestamp" to mapOf("\$lt" to now))
        assertThat(QueryEngine.matches(doc, filter4)).isFalse
    }

    @Test
    fun `temporal comparison with LocalDateTime`() {
        val now = LocalDateTime.now()
        val doc = DocumentUtils.createDocument()
        doc.put("createdAt", now.toInstant(ZoneOffset.UTC).toEpochMilli())

        val startTime = now.minusHours(1)
        val endTime = now.plusHours(1)

        val filter = mapOf(
            "createdAt" to mapOf(
                "\$gte" to startTime,
                "\$lte" to endTime
            )
        )

        assertThat(QueryEngine.matches(doc, filter)).isTrue
    }

    @Test
    fun `temporal comparison with multiple documents`() {
        val baseTime = Instant.parse("2024-01-01T00:00:00Z")

        // Create documents with different timestamps
        val doc1 = DocumentUtils.createDocument()
        doc1.put("timestamp", baseTime.minusSeconds(3600).toEpochMilli())

        val doc2 = DocumentUtils.createDocument()
        doc2.put("timestamp", baseTime.toEpochMilli())

        val doc3 = DocumentUtils.createDocument()
        doc3.put("timestamp", baseTime.plusSeconds(3600).toEpochMilli())

        // Filter for documents in a specific time range
        val startTime = baseTime.minusSeconds(1800)
        val endTime = baseTime.plusSeconds(1800)

        val filter = mapOf(
            "timestamp" to mapOf(
                "\$gte" to startTime,
                "\$lte" to endTime
            )
        )

        // Only doc2 should match
        assertThat(QueryEngine.matches(doc1, filter)).isFalse
        assertThat(QueryEngine.matches(doc2, filter)).isTrue
        assertThat(QueryEngine.matches(doc3, filter)).isFalse
    }
}
