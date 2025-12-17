package com.minileaf.core.storage

import com.fasterxml.jackson.databind.node.ObjectNode

/**
 * Storage engine interface for document persistence.
 * @param ID The type of document identifier (e.g., ObjectId, UUID, String, Long)
 */
interface StorageEngine<ID : Any> {
    /**
     * Inserts or updates a document.
     */
    fun upsert(id: ID, document: ObjectNode)

    /**
     * Finds a document by ID.
     */
    fun findById(id: ID): ObjectNode?

    /**
     * Atomically updates specific fields of a document.
     * Supports MongoDB-style update operators:
     * - $set: Set field values
     * - $unset: Remove fields
     * - $inc: Increment numeric values
     *
     * @param id The document ID
     * @param updates Map of update operations (e.g., mapOf("$set" to mapOf("field" to value)))
     * @return true if document was found and updated, false otherwise
     */
    fun updateFields(id: ID, updates: Map<String, Any?>): Boolean

    /**
     * Deletes a document by ID.
     * @return The deleted document, or null if not found
     */
    fun delete(id: ID): ObjectNode?

    /**
     * Returns all documents in natural order (_id ascending).
     */
    fun findAll(): List<Pair<ID, ObjectNode>>

    /**
     * Returns documents with pagination.
     */
    fun findAll(skip: Int, limit: Int): List<Pair<ID, ObjectNode>>

    /**
     * Returns the total count of documents.
     */
    fun count(): Long

    /**
     * Returns the count of documents matching the given predicate.
     * This method streams documents without loading all into memory.
     * @param predicate The predicate to test each document against
     */
    fun countMatching(predicate: (ObjectNode) -> Boolean): Long

    /**
     * Checks if a document exists.
     */
    fun exists(id: ID): Boolean

    /**
     * Forces a snapshot and clears the WAL.
     */
    fun compact()

    /**
     * Returns storage statistics.
     */
    fun stats(): StorageStats

    /**
     * Closes the storage engine and releases resources.
     */
    fun close()
}

/**
 * Statistics about storage.
 */
data class StorageStats(
    val documentCount: Long,
    val storageBytes: Long,
    val walBytes: Long,
    val lastSnapshotTime: Long?
)
