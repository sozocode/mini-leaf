package com.minileaf.core.storage.cached

import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

/**
 * Thread-safe LRU (Least Recently Used) cache.
 * When capacity is exceeded, the least recently accessed item is evicted.
 *
 * @param maxSize Maximum number of entries to keep in cache
 * @param onEvict Optional callback when an entry is evicted
 */
class LRUCache<K, V>(
    private val maxSize: Int,
    private val onEvict: ((K, V) -> Unit)? = null
) {
    private val lock = ReentrantReadWriteLock()

    // LinkedHashMap with accessOrder=true maintains LRU order
    private val cache = object : LinkedHashMap<K, V>(maxSize, 0.75f, true) {
        override fun removeEldestEntry(eldest: MutableMap.MutableEntry<K, V>?): Boolean {
            val shouldRemove = size > maxSize
            if (shouldRemove && eldest != null) {
                onEvict?.invoke(eldest.key, eldest.value)
            }
            return shouldRemove
        }
    }

    /**
     * Gets a value from the cache. Updates access order.
     * NOTE: Uses write lock because LinkedHashMap with accessOrder=true
     * modifies the map structure on get() to update LRU order.
     */
    fun get(key: K): V? = lock.write {
        cache[key]
    }

    /**
     * Puts a value in the cache. May trigger eviction if at capacity.
     */
    fun put(key: K, value: V): V? = lock.write {
        cache.put(key, value)
    }

    /**
     * Removes a value from the cache.
     */
    fun remove(key: K): V? = lock.write {
        cache.remove(key)
    }

    /**
     * Checks if a key exists in the cache.
     */
    fun contains(key: K): Boolean = lock.read {
        cache.containsKey(key)
    }

    /**
     * Returns the current size of the cache.
     */
    fun size(): Int = lock.read {
        cache.size
    }

    /**
     * Clears all entries from the cache.
     */
    fun clear() = lock.write {
        cache.clear()
    }

    /**
     * Returns cache statistics.
     */
    fun stats(): CacheStats = lock.read {
        CacheStats(
            size = cache.size,
            maxSize = maxSize
        )
    }
}

/**
 * Cache statistics.
 */
data class CacheStats(
    val size: Int,
    val maxSize: Int
) {
    val utilizationPercent: Double
        get() = (size.toDouble() / maxSize * 100)
}
