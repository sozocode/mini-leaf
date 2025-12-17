package com.minileaf.core.repository

import com.fasterxml.jackson.databind.node.ObjectNode
import com.minileaf.core.codec.Codec
import com.minileaf.core.config.MinileafConfig
import com.minileaf.core.document.DocumentUtils
import com.minileaf.core.exception.DocumentTooLargeException
import com.minileaf.core.objectid.ObjectIdUtils
import com.minileaf.core.query.QueryEngine
import com.minileaf.core.storage.IndexManager
import com.minileaf.core.storage.StorageEngine
import mu.KotlinLogging
import org.bson.types.ObjectId
import java.util.*

/**
 * Implementation of the enhanced repository.
 * @param T The entity type
 * @param ID The ID type (e.g., ObjectId, UUID, String, Long)
 */
class RepositoryImpl<T : Any, ID : Comparable<ID>>(
    private val collectionName: String,
    private val codec: Codec<T>,
    private val storageEngine: StorageEngine<ID>,
    private val indexManager: IndexManager<ID>,
    private val config: MinileafConfig,
    private val idGenerator: () -> ID,
    private val idExtractor: (ObjectNode) -> ID?,
    private val idSetter: (ObjectNode, ID) -> Unit
) : IEnhancedSzRepository<T, ID> {

    private val logger = KotlinLogging.logger {}

    override fun save(data: T): T {
        val document = codec.toNode(data)

        // Ensure _id is present
        var id = idExtractor(document)
        if (id == null) {
            id = idGenerator()
            idSetter(document, id)
        }

        // Check document size
        val size = document.toString().length.toLong()
        if (size > config.maxDocumentSize) {
            throw DocumentTooLargeException(size, config.maxDocumentSize)
        }

        // Get old document for index update
        val oldDocument = storageEngine.findById(id)

        // Save to storage
        storageEngine.upsert(id, document)

        // Update indexes
        if (oldDocument == null) {
            indexManager.onInsert(id, document)
        } else {
            indexManager.onUpdate(id, oldDocument, document)
        }

        // Return with _id populated
        return codec.fromNode(document)
    }

    override fun saveAll(data: List<T>): List<T> {
        return data.map { save(it) }
    }

    override fun findById(id: ID): Optional<T> {
        val document = storageEngine.findById(id) ?: return Optional.empty()
        return Optional.of(codec.fromNode(document))
    }

    override fun updateById(id: ID, updates: Map<String, Any?>): Boolean {
        return storageEngine.updateFields(id, updates)
    }

    override fun deleteById(id: ID): Optional<T> {
        val document = storageEngine.delete(id) ?: return Optional.empty()

        // Update indexes
        indexManager.onDelete(id, document)

        return Optional.of(codec.fromNode(document))
    }

    override fun findAll(): List<T> {
        return storageEngine.findAll()
            .map { (_, doc) -> codec.fromNode(doc) }
    }

    override fun findAll(skip: Int, limit: Int): List<T> {
        return storageEngine.findAll(skip, limit)
            .map { (_, doc) -> codec.fromNode(doc) }
    }

    override fun findAll(filter: Map<String, Any>, skip: Int, limit: Int): List<T> {
        // Get all documents and filter them
        // TODO: Optimize with index usage
        val allDocs = storageEngine.findAll()

        val filtered = allDocs
            .filter { (_, doc) -> QueryEngine.matches(doc, filter) }
            .drop(skip)
            .take(limit)
            .map { (_, doc) -> codec.fromNode(doc) }

        return filtered
    }

    override fun exists(id: ID): Boolean {
        return storageEngine.exists(id)
    }

    override fun count(): Long {
        return storageEngine.count()
    }

    override fun count(filter: Map<String, Any>): Long {
        // Optimization 1: Try to use index for simple equality queries
        if (filter.size == 1) {
            val (fieldName, value) = filter.entries.first()

            // Check if this is a simple equality query (not a complex operator like $gte, $in, etc.)
            if (value !is Map<*, *>) {
                val index = indexManager.findIndexForField(fieldName)
                if (index != null) {
                    // Use index for O(log N) lookup instead of O(N) scan
                    logger.debug { "Using index '${index.name()}' for count query on field '$fieldName'" }
                    return index.findEquals(mapOf(fieldName to value)).size.toLong()
                }
            }
        }

        // Optimization 2: Stream documents without loading all into memory
        logger.debug { "Performing streaming count with predicate" }
        return storageEngine.countMatching { doc -> QueryEngine.matches(doc, filter) }
    }

    override fun findByEnumField(fieldName: String, value: Enum<*>): List<T> {
        // Try to use an index if available
        val index = indexManager.findIndexForField(fieldName)
        val ids = if (index != null) {
            index.findEquals(mapOf(fieldName to value))
        } else {
            // Fall back to scan
            logger.warn { "No index found for field '$fieldName', performing full scan" }
            null
        }

        return if (ids != null) {
            ids.mapNotNull { id ->
                storageEngine.findById(id)?.let { codec.fromNode(it) }
            }
        } else {
            // Full scan
            storageEngine.findAll()
                .filter { (_, doc) ->
                    val filter = mapOf(fieldName to value.name)
                    QueryEngine.matches(doc, filter)
                }
                .map { (_, doc) -> codec.fromNode(doc) }
        }
    }

    override fun findByRange(fieldName: String, min: Any, max: Any): List<T> {
        // Try to use an index if available
        val index = indexManager.findIndexForField(fieldName)
        val ids = if (index != null) {
            index.findRange(fieldName, min, max)
        } else {
            // Fall back to scan
            logger.warn { "No index found for field '$fieldName', performing full scan" }
            null
        }

        return if (ids != null) {
            ids.mapNotNull { id ->
                storageEngine.findById(id)?.let { codec.fromNode(it) }
            }
        } else {
            // Full scan
            val filter = mapOf(
                fieldName to mapOf(
                    "\$gte" to min,
                    "\$lte" to max
                )
            )
            storageEngine.findAll()
                .filter { (_, doc) -> QueryEngine.matches(doc, filter) }
                .map { (_, doc) -> codec.fromNode(doc) }
        }
    }
}
