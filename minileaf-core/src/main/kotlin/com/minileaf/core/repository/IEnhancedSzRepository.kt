package com.minileaf.core.repository

import java.util.*

/**
 * Enhanced repository interface for CRUD operations and queries.
 * @param T The entity type
 * @param ID The ID type (must be Comparable)
 */
interface IEnhancedSzRepository<T, ID : Comparable<ID>> {
    /**
     * Saves (upserts) a single entity. If _id is absent, generates one.
     */
    fun save(data: T): T

    /**
     * Saves (upserts) multiple entities in batch.
     */
    fun saveAll(data: List<T>): List<T>

    /**
     * Finds an entity by its ID.
     */
    fun findById(id: ID): Optional<T>

    /**
     * Atomically updates specific fields of a document by ID.
     * Supports MongoDB-style update operators:
     * - $set: Set field values
     * - $unset: Remove fields
     * - $inc: Increment numeric values
     *
     * Example:
     * ```kotlin
     * repository.updateById(id, mapOf(
     *     "$set" to mapOf(
     *         "name" to "New Name",
     *         "age" to 30
     *     ),
     *     "$inc" to mapOf("visits" to 1)
     * ))
     * ```
     *
     * @param id The entity ID
     * @param updates Map of update operations
     * @return true if entity was found and updated, false otherwise
     */
    fun updateById(id: ID, updates: Map<String, Any?>): Boolean

    /**
     * Deletes an entity by its ID and returns it if found.
     */
    fun deleteById(id: ID): Optional<T>

    /**
     * Returns all entities in natural order (_id ascending).
     */
    fun findAll(): List<T>

    /**
     * Returns entities with pagination in natural order.
     */
    fun findAll(skip: Int, limit: Int): List<T>

    /**
     * Finds entities matching the given filter with pagination.
     * @param filter Map-based query filter (Mongo-like syntax)
     */
    fun findAll(filter: Map<String, Any>, skip: Int, limit: Int): List<T>

    /**
     * Checks if an entity with the given ID exists.
     */
    fun exists(id: ID): Boolean

    /**
     * Returns the total count of entities.
     */
    fun count(): Long

    /**
     * Returns the count of entities matching the given filter.
     * @param filter Map-based query filter (Mongo-like syntax)
     */
    fun count(filter: Map<String, Any>): Long

    /**
     * Finds entities by an enum field value.
     * Requires an index on the field for optimal performance.
     */
    fun findByEnumField(fieldName: String, value: Enum<*>): List<T>

    /**
     * Finds entities within a range for the given field (inclusive).
     * Requires an index on the field for optimal performance.
     */
    fun findByRange(fieldName: String, min: Any, max: Any): List<T>
}
