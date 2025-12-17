package com.minileaf.core.storage.memory

import com.fasterxml.jackson.databind.node.ObjectNode
import com.minileaf.core.document.DocumentUtils
import com.minileaf.core.exception.DuplicateKeyException
import com.minileaf.core.index.IndexKey
import com.minileaf.core.storage.Index
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

/**
 * B-tree-based index implementation using ConcurrentSkipListMap.
 * Supports single and compound indexes with range queries.
 * @param ID The type of document identifier
 */
class BTreeIndex<ID : Any>(
    private val indexName: String,
    private val indexKey: IndexKey,
    private val unique: Boolean = false,
    private val enumOptimized: Boolean = false
) : Index<ID> {

    // For enum-optimized indexes, use a hash map
    private val hashIndex: ConcurrentHashMap<String, MutableSet<ID>>? =
        if (enumOptimized && indexKey.isSingleField()) ConcurrentHashMap() else null

    // For range queries, use a sorted map
    // Key: concatenated field values as comparable key, Value: set of IDs
    private val sortedIndex: ConcurrentSkipListMap<IndexKeyValue, MutableSet<ID>>? =
        if (!enumOptimized) ConcurrentSkipListMap() else null

    private val lock = ReentrantReadWriteLock()

    override fun name(): String = indexName

    override fun key(): IndexKey = indexKey

    override fun isUnique(): Boolean = unique

    override fun insert(id: ID, document: ObjectNode) = lock.write {
        val keyValue = extractKeyValue(document) ?: return@write

        if (hashIndex != null) {
            // Enum-optimized hash index
            val key = keyValue.values.first().toString()
            if (unique) {
                val existingIds = hashIndex[key]
                // Only throw error if key exists with a DIFFERENT id
                if (existingIds != null && existingIds.any { it != id }) {
                    throw DuplicateKeyException(indexName, key)
                }
            }
            hashIndex.computeIfAbsent(key) { ConcurrentHashMap.newKeySet() }.add(id)
        } else if (sortedIndex != null) {
            // B-tree sorted index
            if (unique) {
                val existingIds = sortedIndex[keyValue]
                // Only throw error if key exists with a DIFFERENT id
                if (existingIds != null && existingIds.any { it != id }) {
                    throw DuplicateKeyException(indexName, keyValue)
                }
            }
            sortedIndex.computeIfAbsent(keyValue) { ConcurrentHashMap.newKeySet() }.add(id)
        }
    }

    override fun update(id: ID, oldDocument: ObjectNode?, newDocument: ObjectNode) = lock.write {
        if (oldDocument != null) {
            remove(id, oldDocument)
        }
        insert(id, newDocument)
    }

    override fun remove(id: ID, document: ObjectNode) = lock.write {
        val keyValue = extractKeyValue(document) ?: return@write

        if (hashIndex != null) {
            val key = keyValue.values.first().toString()
            hashIndex[key]?.remove(id)
            if (hashIndex[key]?.isEmpty() == true) {
                hashIndex.remove(key)
            }
        } else if (sortedIndex != null) {
            sortedIndex[keyValue]?.remove(id)
            if (sortedIndex[keyValue]?.isEmpty() == true) {
                sortedIndex.remove(keyValue)
            }
        }
    }

    override fun findEquals(values: Map<String, Any>): Set<ID> = lock.read {
        if (hashIndex != null && indexKey.isSingleField()) {
            // Hash lookup for enum-optimized index
            val fieldName = indexKey.firstField()
            val value = values[fieldName] ?: return@read emptySet()
            val key = when (value) {
                is Enum<*> -> value.name
                else -> value.toString()
            }
            return@read hashIndex[key]?.toSet() ?: emptySet()
        } else if (sortedIndex != null) {
            // B-tree lookup
            val keyValue = buildKeyValue(values) ?: return@read emptySet()
            return@read sortedIndex[keyValue]?.toSet() ?: emptySet()
        }
        emptySet()
    }

    override fun findRange(fieldName: String, min: Any?, max: Any?): Set<ID> = lock.read {
        if (sortedIndex == null || !indexKey.isSingleField() || indexKey.firstField() != fieldName) {
            return@read emptySet()
        }

        val minKey = min?.let { IndexKeyValue(listOf(it as Comparable<Any>)) }
        val maxKey = max?.let { IndexKeyValue(listOf(it as Comparable<Any>)) }

        val subMap = when {
            minKey != null && maxKey != null -> sortedIndex.subMap(minKey, true, maxKey, true)
            minKey != null -> sortedIndex.tailMap(minKey, true)
            maxKey != null -> sortedIndex.headMap(maxKey, true)
            else -> sortedIndex
        }

        val result = mutableSetOf<ID>()
        subMap.values.forEach { result.addAll(it) }
        result
    }

    override fun sizeBytes(): Long {
        val hashSize = hashIndex?.size?.toLong() ?: 0L
        val sortedSize = sortedIndex?.size?.toLong() ?: 0L
        return (hashSize + sortedSize) * 64 // Rough estimate
    }

    override fun clear(): Unit = lock.write {
        hashIndex?.clear()
        sortedIndex?.clear()
    }

    /**
     * Extracts the index key value from a document.
     */
    private fun extractKeyValue(document: ObjectNode): IndexKeyValue? {
        val values = mutableListOf<Comparable<Any>?>()

        for (fieldName in indexKey.fieldNames()) {
            val node = DocumentUtils.getValueByPath(document, fieldName)
            val comparable = DocumentUtils.toComparable(node) as? Comparable<Any>
            values.add(comparable)
        }

        // If any required field is missing, don't index this document
        if (values.any { it == null }) return null

        @Suppress("UNCHECKED_CAST")
        return IndexKeyValue(values as List<Comparable<Any>>)
    }

    /**
     * Builds an index key value from a map of field values.
     */
    private fun buildKeyValue(values: Map<String, Any>): IndexKeyValue? {
        val keyValues = mutableListOf<Comparable<Any>>()

        for (fieldName in indexKey.fieldNames()) {
            val value = values[fieldName] ?: return null
            @Suppress("UNCHECKED_CAST")
            val comparable: Comparable<Any> = when (value) {
                is Comparable<*> -> value as Comparable<Any>
                is Enum<*> -> value.name as Comparable<Any>
                else -> value.toString() as Comparable<Any>
            }
            keyValues.add(comparable)
        }

        return IndexKeyValue(keyValues)
    }
}

/**
 * Composite key for multi-field indexes.
 * Compares fields in order.
 */
data class IndexKeyValue(
    val values: List<Comparable<Any>>
) : Comparable<IndexKeyValue> {

    override fun compareTo(other: IndexKeyValue): Int {
        for (i in values.indices) {
            if (i >= other.values.size) return 1

            @Suppress("UNCHECKED_CAST")
            val cmp = (values[i] as Comparable<Any>).compareTo(other.values[i])
            if (cmp != 0) return cmp
        }
        return values.size.compareTo(other.values.size)
    }

    override fun toString(): String = values.joinToString("::")
}
