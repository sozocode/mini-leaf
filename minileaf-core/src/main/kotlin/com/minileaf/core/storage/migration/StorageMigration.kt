package com.minileaf.core.storage.migration

import com.minileaf.core.config.MinileafConfig
import com.minileaf.core.storage.StorageEngine
import com.minileaf.core.storage.cached.CachedStorageEngine
import com.minileaf.core.storage.file.FileBackedStorageEngine
import com.minileaf.core.storage.memory.InMemoryStorageEngine
import mu.KotlinLogging

/**
 * Utilities for migrating data between different storage engines.
 */
object StorageMigration {
    private val logger = KotlinLogging.logger {}

    /**
     * Migrates data from one storage engine to another.
     * Reads all documents from source and writes to destination.
     *
     * @param source Source storage engine (will be read from)
     * @param destination Destination storage engine (will be written to)
     * @param batchSize Number of documents to migrate at a time
     * @return Number of documents migrated
     */
    fun <ID : Comparable<ID>> migrate(
        source: StorageEngine<ID>,
        destination: StorageEngine<ID>,
        batchSize: Int = 1000
    ): Long {
        logger.info { "Starting storage migration..." }

        var migratedCount = 0L
        val allDocs = source.findAll()

        logger.info { "Found ${allDocs.size} documents to migrate" }

        allDocs.chunked(batchSize).forEachIndexed { chunkIndex, chunk ->
            chunk.forEach { (id, doc) ->
                destination.upsert(id, doc)
                migratedCount++
            }

            if ((chunkIndex + 1) % 10 == 0 || chunkIndex == allDocs.size / batchSize) {
                logger.info { "Migrated $migratedCount / ${allDocs.size} documents" }
            }
        }

        logger.info { "Migration complete: $migratedCount documents migrated" }
        return migratedCount
    }

    /**
     * Migrates a collection from FileBackedStorageEngine to CachedStorageEngine.
     *
     * Example:
     * ```
     * val oldConfig = MinileafConfig()
     * val newConfig = MinileafConfig(cacheSize = 10_000)
     *
     * StorageMigration.migrateFileBackedToCached<ObjectId>(
     *     collectionName = "users",
     *     oldConfig = oldConfig,
     *     newConfig = newConfig,
     *     idSerializer = { it.toHexString() },
     *     idDeserializer = { ObjectId(it) }
     * )
     * ```
     */
    fun <ID : Comparable<ID>> migrateFileBackedToCached(
        collectionName: String,
        oldConfig: MinileafConfig,
        newConfig: MinileafConfig,
        idSerializer: (ID) -> String,
        idDeserializer: (String) -> ID
    ): Long {
        require(newConfig.cacheSize != null) {
            "New config must have cacheSize set for CachedStorageEngine"
        }

        logger.info { "Migrating collection '$collectionName' from FileBackedStorageEngine to CachedStorageEngine" }

        val source = FileBackedStorageEngine(
            collectionName,
            oldConfig,
            idSerializer,
            idDeserializer
        )

        val destination = CachedStorageEngine(
            collectionName,
            newConfig,
            newConfig.cacheSize,
            idSerializer,
            idDeserializer
        )

        try {
            val count = migrate(source, destination)

            source.close()
            destination.close()

            logger.info { "Migration successful: $count documents" }
            return count
        } catch (e: Exception) {
            logger.error(e) { "Migration failed" }
            source.close()
            destination.close()
            throw e
        }
    }

    /**
     * Migrates a collection from CachedStorageEngine to FileBackedStorageEngine.
     */
    fun <ID : Comparable<ID>> migrateCachedToFileBacked(
        collectionName: String,
        oldConfig: MinileafConfig,
        newConfig: MinileafConfig,
        idSerializer: (ID) -> String,
        idDeserializer: (String) -> ID
    ): Long {
        require(oldConfig.cacheSize != null) {
            "Old config must have cacheSize set (indicating CachedStorageEngine)"
        }

        logger.info { "Migrating collection '$collectionName' from CachedStorageEngine to FileBackedStorageEngine" }

        val source = CachedStorageEngine(
            collectionName,
            oldConfig,
            oldConfig.cacheSize,
            idSerializer,
            idDeserializer
        )

        val destination = FileBackedStorageEngine(
            collectionName,
            newConfig,
            idSerializer,
            idDeserializer
        )

        try {
            val count = migrate(source, destination)

            source.close()
            destination.close()

            logger.info { "Migration successful: $count documents" }
            return count
        } catch (e: Exception) {
            logger.error(e) { "Migration failed" }
            source.close()
            destination.close()
            throw e
        }
    }
}
