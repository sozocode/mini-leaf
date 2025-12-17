package com.minileaf.core.index

import com.fasterxml.jackson.databind.node.ObjectNode
import com.minileaf.core.document.DocumentUtils
import com.minileaf.core.storage.Index
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

/**
 * TTL (Time-To-Live) index that automatically expires documents after a specified duration.
 * Monitors a date field and marks documents for deletion when they expire.
 *
 * @param ID The type of document identifier
 * @param indexName Name of the index
 * @param indexKey The index key (must be a single date/timestamp field)
 * @param expireAfterSeconds Duration in seconds after which documents expire
 * @param onExpire Callback invoked when documents expire (receives ID)
 */
class TTLIndex<ID : Any>(
    private val indexName: String,
    private val indexKey: IndexKey,
    private val expireAfterSeconds: Long,
    private val onExpire: (ID) -> Unit
) : Index<ID> {

    private val documents = ConcurrentHashMap<ID, Instant>()
    private val lock = ReentrantReadWriteLock()

    init {
        require(indexKey.isSingleField()) { "TTL index must be on a single field" }
        require(expireAfterSeconds > 0) { "expireAfterSeconds must be positive" }
    }

    override fun name(): String = indexName

    override fun key(): IndexKey = indexKey

    override fun isUnique(): Boolean = false

    override fun insert(id: ID, document: ObjectNode) = lock.write {
        val timestamp = extractTimestamp(document)
        if (timestamp != null) {
            documents[id] = timestamp
        }
    }

    override fun update(id: ID, oldDocument: ObjectNode?, newDocument: ObjectNode) = lock.write {
        documents.remove(id)
        val timestamp = extractTimestamp(newDocument)
        if (timestamp != null) {
            documents[id] = timestamp
        }
    }

    override fun remove(id: ID, document: ObjectNode): Unit = lock.write {
        documents.remove(id)
    }

    override fun findEquals(values: Map<String, Any>): Set<ID> = emptySet()

    override fun findRange(fieldName: String, min: Any?, max: Any?): Set<ID> = emptySet()

    override fun sizeBytes(): Long = documents.size.toLong() * 64

    override fun clear(): Unit = lock.write {
        documents.clear()
    }

    /**
     * Checks for expired documents and invokes the expiration callback.
     * This should be called periodically by a background task.
     */
    fun checkExpired() = lock.read {
        val now = Instant.now()
        val expired = documents.entries
            .filter { (_, timestamp) ->
                val expireAt = timestamp.plusSeconds(expireAfterSeconds)
                now.isAfter(expireAt)
            }
            .map { it.key }

        expired.forEach { id ->
            lock.write {
                documents.remove(id)
            }
            try {
                onExpire(id)
            } catch (e: Exception) {
                // Log error but continue processing
            }
        }
    }

    /**
     * Extracts the timestamp from the indexed field.
     */
    private fun extractTimestamp(document: ObjectNode): Instant? {
        val fieldName = indexKey.firstField()
        val node = DocumentUtils.getValueByPath(document, fieldName) ?: return null

        return when {
            node.isTextual -> try {
                Instant.parse(node.asText())
            } catch (e: Exception) {
                null
            }
            node.isNumber -> Instant.ofEpochMilli(node.asLong())
            else -> null
        }
    }
}
