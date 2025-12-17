package com.minileaf.core.admin

import com.minileaf.core.index.IndexInfo
import com.minileaf.core.index.IndexKey
import com.minileaf.core.index.IndexOptions

/**
 * Administrative interface for managing a collection.
 */
interface CollectionAdmin {
    /**
     * Creates an index on the collection.
     * @return The name of the created index
     */
    fun createIndex(key: IndexKey, options: IndexOptions = IndexOptions()): String

    /**
     * Drops an index by name.
     */
    fun dropIndex(name: String)

    /**
     * Lists all indexes on the collection.
     */
    fun listIndexes(): List<IndexInfo>
}
