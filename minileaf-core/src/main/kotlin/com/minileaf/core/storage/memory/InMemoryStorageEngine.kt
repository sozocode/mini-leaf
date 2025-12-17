package com.minileaf.core.storage.memory

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.minileaf.core.storage.StorageEngine
import com.minileaf.core.storage.StorageStats
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

/**
 * In-memory storage engine using a sorted map (skip list).
 * Provides O(log N) operations for all primary key operations.
 * @param ID The type of document identifier - must be Comparable for sorted storage
 */
class InMemoryStorageEngine<ID : Comparable<ID>> : StorageEngine<ID> {
    // Primary storage: ID -> Document
    private val documents = ConcurrentSkipListMap<ID, ObjectNode>()

    // Read-write lock for consistency
    private val lock = ReentrantReadWriteLock()

    // Stats
    private val storageBytesEstimate = AtomicLong(0)

    // ObjectMapper for proper serialization of complex types
    private val objectMapper = ObjectMapper().apply {
        registerKotlinModule()
        findAndRegisterModules()
    }

    override fun upsert(id: ID, document: ObjectNode): Unit = lock.write {
        val oldDoc = documents.put(id, document)

        // Update size estimate
        if (oldDoc == null) {
            storageBytesEstimate.addAndGet(estimateSize(document))
        } else {
            val oldSize = estimateSize(oldDoc)
            val newSize = estimateSize(document)
            storageBytesEstimate.addAndGet(newSize - oldSize)
        }
    }

    override fun findById(id: ID): ObjectNode? = lock.read {
        documents[id]?.deepCopy()
    }

    override fun updateFields(id: ID, updates: Map<String, Any?>): Boolean = lock.write {
        val doc = documents[id] ?: return@write false

        // Apply update operators
        for ((operator, value) in updates) {
            when (operator) {
                "\$set" -> {
                    @Suppress("UNCHECKED_CAST")
                    val fields = value as? Map<String, Any?> ?: continue
                    for ((fieldPath, fieldValue) in fields) {
                        setField(doc, fieldPath, fieldValue)
                    }
                }
                "\$unset" -> {
                    @Suppress("UNCHECKED_CAST")
                    val fields = value as? Map<String, Any> ?: continue
                    for (fieldPath in fields.keys) {
                        unsetField(doc, fieldPath)
                    }
                }
                "\$inc" -> {
                    @Suppress("UNCHECKED_CAST")
                    val fields = value as? Map<String, Any> ?: continue
                    for ((fieldPath, increment) in fields) {
                        incrementField(doc, fieldPath, increment)
                    }
                }
            }
        }

        // Update size estimate
        val newSize = estimateSize(doc)
        storageBytesEstimate.set(storageBytesEstimate.get() + newSize - estimateSize(doc))

        true
    }

    private fun setField(doc: ObjectNode, fieldPath: String, value: Any?) {
        val parts = fieldPath.split(".")
        var current = doc

        // Navigate to parent
        for (i in 0 until parts.size - 1) {
            val part = parts[i]
            if (!current.has(part) || !current.get(part).isObject) {
                current.putObject(part)
            }
            current = current.get(part) as ObjectNode
        }

        // Set the field value
        val fieldName = parts.last()
        when (value) {
            null -> current.putNull(fieldName)
            is String -> current.put(fieldName, value)
            is Int -> current.put(fieldName, value)
            is Long -> current.put(fieldName, value)
            is Double -> current.put(fieldName, value)
            is Boolean -> current.put(fieldName, value)
            else -> {
                // For complex types (Date, Instant, etc.), use ObjectMapper to properly serialize
                // This ensures the value is serialized the same way as when saving a full entity
                val jsonNode = objectMapper.valueToTree<com.fasterxml.jackson.databind.JsonNode>(value)
                current.set<com.fasterxml.jackson.databind.JsonNode>(fieldName, jsonNode)
            }
        }
    }

    private fun unsetField(doc: ObjectNode, fieldPath: String) {
        val parts = fieldPath.split(".")
        var current = doc

        // Navigate to parent
        for (i in 0 until parts.size - 1) {
            val part = parts[i]
            if (!current.has(part) || !current.get(part).isObject) {
                return  // Path doesn't exist
            }
            current = current.get(part) as ObjectNode
        }

        // Remove the field
        current.remove(parts.last())
    }

    private fun incrementField(doc: ObjectNode, fieldPath: String, increment: Any) {
        val parts = fieldPath.split(".")
        var current = doc

        // Navigate to parent
        for (i in 0 until parts.size - 1) {
            val part = parts[i]
            if (!current.has(part) || !current.get(part).isObject) {
                current.putObject(part)
            }
            current = current.get(part) as ObjectNode
        }

        // Increment the field
        val fieldName = parts.last()

        // Get current value and convert to appropriate type
        val currentNum: Number = if (current.has(fieldName)) {
            val node = current.get(fieldName)
            when {
                node.isInt -> node.asInt()
                node.isLong -> node.asLong()
                node.isDouble -> node.asDouble()
                node.isFloatingPointNumber -> node.asDouble()
                else -> 0
            }
        } else {
            0
        }

        // Apply increment based on increment type
        when (increment) {
            is Int -> {
                val result = currentNum.toInt() + increment
                current.put(fieldName, result)
            }
            is Long -> {
                val result = currentNum.toLong() + increment
                current.put(fieldName, result)
            }
            is Double -> {
                val result = currentNum.toDouble() + increment
                current.put(fieldName, result)
            }
        }
    }

    override fun delete(id: ID): ObjectNode? = lock.write {
        val removed = documents.remove(id)
        if (removed != null) {
            storageBytesEstimate.addAndGet(-estimateSize(removed))
        }
        removed
    }

    override fun findAll(): List<Pair<ID, ObjectNode>> = lock.read {
        documents.map { (id, doc) -> id to doc.deepCopy() }
    }

    override fun findAll(skip: Int, limit: Int): List<Pair<ID, ObjectNode>> = lock.read {
        documents.entries
            .drop(skip)
            .take(limit)
            .map { (id, doc) -> id to doc.deepCopy() }
    }

    override fun count(): Long = documents.size.toLong()

    override fun countMatching(predicate: (ObjectNode) -> Boolean): Long = lock.read {
        var count = 0L
        for ((_, doc) in documents) {
            if (predicate(doc)) {
                count++
            }
        }
        count
    }

    override fun exists(id: ID): Boolean = documents.containsKey(id)

    override fun compact() {
        // No-op for in-memory storage
    }

    override fun stats(): StorageStats = lock.read {
        StorageStats(
            documentCount = documents.size.toLong(),
            storageBytes = storageBytesEstimate.get(),
            walBytes = 0,
            lastSnapshotTime = null
        )
    }

    override fun close() {
        // No-op for in-memory storage
    }

    /**
     * Estimates the size of a document in bytes.
     */
    private fun estimateSize(document: ObjectNode): Long {
        return document.toString().length.toLong()
    }
}
