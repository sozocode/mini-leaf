package com.minileaf.core

import com.minileaf.core.admin.CollectionAdmin
import com.minileaf.core.admin.CollectionHandle
import com.minileaf.core.admin.CollectionStats
import com.minileaf.core.storage.IndexManager
import com.minileaf.core.storage.StorageEngine

/**
 * Implementation of CollectionHandle.
 * @param ID The type of document identifier
 */
class CollectionHandleImpl<ID : Comparable<ID>>(
    private val collectionName: String,
    private val storageEngine: StorageEngine<ID>,
    private val indexManager: IndexManager<ID>
) : CollectionHandle {

    override fun name(): String = collectionName

    override fun admin(): CollectionAdmin = indexManager

    override fun stats(): CollectionStats {
        val storageStats = storageEngine.stats()
        val indexes = indexManager.allIndexes()

        val indexSizes = indexes.mapValues { (_, index) -> index.sizeBytes() }

        return CollectionStats(
            name = collectionName,
            documentCount = storageStats.documentCount,
            storageBytes = storageStats.storageBytes,
            indexCount = indexes.size,
            indexSizes = indexSizes,
            lastSnapshotTime = storageStats.lastSnapshotTime
        )
    }

    override fun compact() {
        storageEngine.compact()
    }
}
