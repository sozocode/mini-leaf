package com.minileaf.core.storage.cached

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.minileaf.core.config.MinileafConfig
import com.minileaf.core.storage.StorageEngine
import com.minileaf.core.storage.StorageStats
import mu.KotlinLogging
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

/**
 * Cached storage engine with LRU cache and disk persistence.
 * Keeps hot documents in memory (bounded size) and cold documents on disk.
 *
 * Benefits:
 * - Bounded memory usage (only cacheSize documents in RAM)
 * - Scalable to large datasets (disk-backed)
 * - Good performance for hot data (LRU cache)
 *
 * Trade-offs:
 * - Cache misses require disk I/O
 * - Slower than pure in-memory for datasets that fit in RAM
 *
 * @param collectionName Name of the collection
 * @param config Minileaf configuration
 * @param cacheSize Maximum number of documents to keep in memory
 * @param idSerializer Function to serialize ID to string
 * @param idDeserializer Function to deserialize string to ID
 */
class CachedStorageEngine<ID : Comparable<ID>>(
    private val collectionName: String,
    private val config: MinileafConfig,
    private val cacheSize: Int = 10_000,
    private val idSerializer: (ID) -> String,
    private val idDeserializer: (String) -> ID
) : StorageEngine<ID> {

    private val logger = KotlinLogging.logger {}

    // LRU cache for hot documents
    private val cache = LRUCache<ID, ObjectNode>(
        maxSize = cacheSize,
        onEvict = { id, _ ->
            logger.trace { "Evicted document $id from cache" }
            cacheEvictions.incrementAndGet()
        }
    )

    // Disk store for all documents
    private val diskStore: DiskStore<ID>

    // Statistics
    private val cacheHits = AtomicLong(0)
    private val cacheMisses = AtomicLong(0)
    private val cacheEvictions = AtomicLong(0)

    private val lock = ReentrantReadWriteLock()

    // ObjectMapper for proper serialization of complex types
    private val objectMapper = ObjectMapper().apply {
        registerKotlinModule()
        findAndRegisterModules()
    }

    init {
        val dataDir = config.dataDir.resolve("collections")
        dataDir.toFile().mkdirs()

        val dataFile = dataDir.resolve("$collectionName.data")
        diskStore = DiskStore(dataFile, idSerializer, idDeserializer, config.syncOnWrite)

        logger.info {
            "Initialized cached storage for collection '$collectionName' " +
                    "(cache: $cacheSize docs, disk: ${diskStore.count()} docs)"
        }
    }

    override fun upsert(id: ID, document: ObjectNode): Unit = lock.write {
        // Write-through: write to both cache and disk
        val docCopy = document.deepCopy()
        cache.put(id, docCopy)
        diskStore.write(id, docCopy)
    }

    override fun findById(id: ID): ObjectNode? {
        // Try cache first (cache has its own locking)
        cache.get(id)?.let {
            cacheHits.incrementAndGet()
            return it.deepCopy()
        }

        // Cache miss - load from disk (diskStore has its own locking)
        cacheMisses.incrementAndGet()
        val doc = diskStore.read(id) ?: return null

        // Populate cache with double-check
        // Use lock to coordinate potential concurrent cache population
        val finalDoc = lock.write {
            // Re-check cache: another thread might have updated it while we were reading from disk
            val cached = cache.get(id)
            if (cached != null) {
                // Use the cached version (it's fresher or same)
                cached
            } else {
                // Populate cache with our disk read
                val docCopy = doc.deepCopy()
                cache.put(id, docCopy)
                docCopy
            }
        }

        return finalDoc.deepCopy()
    }

    override fun updateFields(id: ID, updates: Map<String, Any?>): Boolean = lock.write {
        // Check cache first, then fall back to disk
        val doc = cache.get(id)?.deepCopy() ?: diskStore.read(id) ?: return@write false

        // Apply updates (reuse logic from InMemoryStorageEngine)
        applyUpdates(doc, updates)

        // Write back to both cache and disk
        val docCopy = doc.deepCopy()
        cache.put(id, docCopy)
        diskStore.write(id, docCopy)

        true
    }

    private fun applyUpdates(doc: ObjectNode, updates: Map<String, Any?>) {
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
    }

    private fun setField(doc: ObjectNode, fieldPath: String, value: Any?) {
        val parts = fieldPath.split(".")
        var current = doc

        for (i in 0 until parts.size - 1) {
            val part = parts[i]
            if (!current.has(part) || !current.get(part).isObject) {
                current.putObject(part)
            }
            current = current.get(part) as ObjectNode
        }

        val fieldName = parts.last()
        when (value) {
            null -> current.putNull(fieldName)
            is String -> current.put(fieldName, value)
            is Int -> current.put(fieldName, value)
            is Long -> current.put(fieldName, value)
            is Double -> current.put(fieldName, value)
            is Boolean -> current.put(fieldName, value)
            else -> {
                // For complex types (Date, Instant, UUID, etc.), use ObjectMapper to properly serialize
                // This ensures the value is serialized the same way as when saving a full entity
                val jsonNode = objectMapper.valueToTree<com.fasterxml.jackson.databind.JsonNode>(value)
                current.set<com.fasterxml.jackson.databind.JsonNode>(fieldName, jsonNode)
            }
        }
    }

    private fun unsetField(doc: ObjectNode, fieldPath: String) {
        val parts = fieldPath.split(".")
        var current = doc

        for (i in 0 until parts.size - 1) {
            val part = parts[i]
            if (!current.has(part) || !current.get(part).isObject) {
                return
            }
            current = current.get(part) as ObjectNode
        }

        current.remove(parts.last())
    }

    private fun incrementField(doc: ObjectNode, fieldPath: String, increment: Any) {
        val parts = fieldPath.split(".")
        var current = doc

        for (i in 0 until parts.size - 1) {
            val part = parts[i]
            if (!current.has(part) || !current.get(part).isObject) {
                current.putObject(part)
            }
            current = current.get(part) as ObjectNode
        }

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
        // Remove from cache
        cache.remove(id)

        // Load from disk before deleting (for return value)
        val doc = diskStore.read(id)

        // Delete from disk
        diskStore.delete(id)

        doc
    }

    override fun findAll(): List<Pair<ID, ObjectNode>> {
        // DiskStore.readAll() handles its own locking internally
        return diskStore.readAll()
    }

    override fun findAll(skip: Int, limit: Int): List<Pair<ID, ObjectNode>> {
        // Use consistent method to avoid stale reads during compact()
        val ids = diskStore.allIdsConsistent()
            .sorted()
            .drop(skip)
            .take(limit)

        // Load each document (findById handles its own locking)
        return ids.mapNotNull { id ->
            findById(id)?.let { id to it }
        }
    }

    override fun count(): Long {
        // Use consistent method for accurate count
        return diskStore.countConsistent()
    }

    override fun countMatching(predicate: (ObjectNode) -> Boolean): Long {
        // DiskStore.forEach() handles its own locking internally
        var count = 0L
        diskStore.forEach { _, doc ->
            if (predicate(doc)) {
                count++
            }
        }
        return count
    }

    override fun exists(id: ID): Boolean {
        // Use consistent method to avoid stale reads during compact()
        return diskStore.existsConsistent(id)
    }

    override fun compact() = lock.write {
        logger.info { "Compacting cached storage for '$collectionName'" }
        diskStore.compact()
    }

    override fun stats(): StorageStats {
        val diskSize = diskStore.fileSize()

        return StorageStats(
            documentCount = diskStore.count(),
            storageBytes = diskSize,
            walBytes = 0,
            lastSnapshotTime = null
        )
    }

    override fun close() {
        logger.info {
            "Closing cached storage for '$collectionName' " +
                    "(hits: ${cacheHits.get()}, misses: ${cacheMisses.get()}, " +
                    "evictions: ${cacheEvictions.get()})"
        }

        cache.clear()
        diskStore.close()
    }

    /**
     * Returns cache statistics for monitoring.
     */
    fun cacheStats(): CachedStorageStats {
        val cacheInfo = cache.stats()
        return CachedStorageStats(
            cacheSize = cacheInfo.size,
            cacheMaxSize = cacheInfo.maxSize,
            cacheUtilization = cacheInfo.utilizationPercent,
            cacheHits = cacheHits.get(),
            cacheMisses = cacheMisses.get(),
            cacheEvictions = cacheEvictions.get(),
            hitRate = calculateHitRate()
        )
    }

    private fun calculateHitRate(): Double {
        val hits = cacheHits.get()
        val misses = cacheMisses.get()
        val total = hits + misses
        return if (total > 0) hits.toDouble() / total * 100 else 0.0
    }
}

/**
 * Cache statistics for monitoring and tuning.
 */
data class CachedStorageStats(
    val cacheSize: Int,
    val cacheMaxSize: Int,
    val cacheUtilization: Double,
    val cacheHits: Long,
    val cacheMisses: Long,
    val cacheEvictions: Long,
    val hitRate: Double
)
