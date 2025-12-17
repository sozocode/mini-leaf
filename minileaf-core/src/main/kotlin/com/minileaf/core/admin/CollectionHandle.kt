package com.minileaf.core.admin

/**
 * Handle to a collection for administrative operations.
 */
interface CollectionHandle {
    /**
     * Returns the collection name.
     */
    fun name(): String

    /**
     * Returns the admin interface for this collection.
     */
    fun admin(): CollectionAdmin

    /**
     * Returns statistics about this collection.
     */
    fun stats(): CollectionStats

    /**
     * Forces a snapshot and WAL reset.
     */
    fun compact()
}

/**
 * Statistics about a collection.
 */
data class CollectionStats(
    val name: String,
    val documentCount: Long,
    val storageBytes: Long,
    val indexCount: Int,
    val indexSizes: Map<String, Long>,
    val lastSnapshotTime: Long? = null
)
