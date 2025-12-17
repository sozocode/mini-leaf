package com.minileaf.core.storage

import com.fasterxml.jackson.databind.node.ObjectNode
import com.minileaf.core.admin.CollectionAdmin
import com.minileaf.core.exception.MinileafException
import com.minileaf.core.index.IndexInfo
import com.minileaf.core.index.IndexKey
import com.minileaf.core.index.IndexOptions
import com.minileaf.core.storage.memory.BTreeIndex
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

/**
 * Manages indexes for a collection.
 * @param ID The type of document identifier
 */
class IndexManager<ID : Any>(
    private val collectionName: String,
    private val storageEngine: StorageEngine<ID>
) : CollectionAdmin {

    // Index name -> Index
    private val indexes = ConcurrentHashMap<String, Index<ID>>()

    private val lock = ReentrantReadWriteLock()

    init {
        // Create primary index on _id (always present)
        val primaryIndex = BTreeIndex<ID>(
            indexName = "_id_",
            indexKey = IndexKey("_id", 1),
            unique = true,
            enumOptimized = false
        )
        indexes["_id_"] = primaryIndex
    }

    override fun createIndex(key: IndexKey, options: IndexOptions): String = lock.write {
        val indexName = options.name ?: generateIndexName(key)

        // Check if index already exists
        if (indexes.containsKey(indexName)) {
            throw MinileafException("Index '$indexName' already exists")
        }

        // Create the index
        val index = BTreeIndex<ID>(
            indexName = indexName,
            indexKey = key,
            unique = options.unique,
            enumOptimized = options.enumOptimized
        )

        // Build the index with existing documents
        buildIndex(index)

        indexes[indexName] = index
        indexName
    }

    override fun dropIndex(name: String): Unit = lock.write {
        if (name == "_id_") {
            throw MinileafException("Cannot drop primary index '_id_'")
        }

        indexes.remove(name) ?: throw MinileafException("Index '$name' not found")
    }

    override fun listIndexes(): List<IndexInfo> = lock.read {
        indexes.values.map { index ->
            IndexInfo(
                name = index.name(),
                key = index.key(),
                unique = index.isUnique(),
                enumOptimized = false // TODO: track this in Index interface
            )
        }
    }

    /**
     * Notifies all indexes of a document insert.
     */
    fun onInsert(id: ID, document: ObjectNode) = lock.read {
        indexes.values.forEach { index ->
            try {
                index.insert(id, document)
            } catch (e: Exception) {
                // Rollback on error
                indexes.values.forEach { it.remove(id, document) }
                throw e
            }
        }
    }

    /**
     * Notifies all indexes of a document update.
     */
    fun onUpdate(id: ID, oldDocument: ObjectNode?, newDocument: ObjectNode) = lock.read {
        indexes.values.forEach { index ->
            try {
                index.update(id, oldDocument, newDocument)
            } catch (e: Exception) {
                // Rollback on error
                if (oldDocument != null) {
                    indexes.values.forEach { it.update(id, newDocument, oldDocument) }
                }
                throw e
            }
        }
    }

    /**
     * Notifies all indexes of a document delete.
     */
    fun onDelete(id: ID, document: ObjectNode) = lock.read {
        indexes.values.forEach { index ->
            index.remove(id, document)
        }
    }

    /**
     * Finds an index that can be used for a query on the given field.
     */
    fun findIndexForField(fieldName: String): Index<ID>? = lock.read {
        indexes.values.find { index ->
            index.key().isSingleField() && index.key().firstField() == fieldName
        }
    }

    /**
     * Finds an index that matches the given fields (for compound queries).
     */
    fun findIndexForFields(fieldNames: Set<String>): Index<ID>? = lock.read {
        indexes.values.find { index ->
            val indexFields = index.key().fieldNames().toSet()
            indexFields == fieldNames
        }
    }

    /**
     * Returns all indexes.
     */
    fun allIndexes(): Map<String, Index<ID>> = lock.read {
        indexes.toMap()
    }

    /**
     * Builds an index by scanning all documents in storage.
     */
    private fun buildIndex(index: Index<ID>) {
        val documents = storageEngine.findAll()
        for ((id, document) in documents) {
            index.insert(id, document)
        }
    }

    /**
     * Generates a default index name from the key.
     */
    private fun generateIndexName(key: IndexKey): String {
        return key.fields.entries.joinToString("_") { (field, direction) ->
            "${field}_${direction}"
        }
    }
}
