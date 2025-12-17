package com.minileaf.core.storage

import com.fasterxml.jackson.databind.node.ObjectNode
import com.minileaf.core.index.IndexKey

/**
 * Interface for an index on a collection.
 * @param ID The type of document identifier (e.g., ObjectId, UUID, String, Long)
 */
interface Index<ID : Any> {
    /**
     * Returns the index name.
     */
    fun name(): String

    /**
     * Returns the index key.
     */
    fun key(): IndexKey

    /**
     * Returns whether this index is unique.
     */
    fun isUnique(): Boolean

    /**
     * Inserts a document into the index.
     */
    fun insert(id: ID, document: ObjectNode)

    /**
     * Updates a document in the index (remove old, insert new).
     */
    fun update(id: ID, oldDocument: ObjectNode?, newDocument: ObjectNode)

    /**
     * Removes a document from the index.
     */
    fun remove(id: ID, document: ObjectNode)

    /**
     * Finds document IDs matching an equality condition on indexed fields.
     */
    fun findEquals(values: Map<String, Any>): Set<ID>

    /**
     * Finds document IDs matching a range condition on the first indexed field.
     */
    fun findRange(fieldName: String, min: Any?, max: Any?): Set<ID>

    /**
     * Returns the size of the index in bytes (approximate).
     */
    fun sizeBytes(): Long

    /**
     * Clears the index.
     */
    fun clear()
}
