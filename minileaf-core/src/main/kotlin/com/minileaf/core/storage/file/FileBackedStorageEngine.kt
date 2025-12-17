package com.minileaf.core.storage.file

import com.fasterxml.jackson.databind.node.ObjectNode
import com.minileaf.core.config.MinileafConfig
import com.minileaf.core.storage.StorageEngine
import com.minileaf.core.storage.StorageStats
import com.minileaf.core.storage.memory.InMemoryStorageEngine
import mu.KotlinLogging
import java.nio.file.Path
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

/**
 * File-backed storage engine with WAL and snapshots.
 * Provides crash-safe persistence with automatic recovery.
 * @param ID The type of document identifier - must be Comparable for sorted storage
 */
class FileBackedStorageEngine<ID : Comparable<ID>>(
    private val collectionName: String,
    private val config: MinileafConfig,
    private val idSerializer: (ID) -> String,
    private val idDeserializer: (String) -> ID
) : StorageEngine<ID> {

    private val logger = KotlinLogging.logger {}

    // In-memory storage for fast access
    private val memoryStorage = InMemoryStorageEngine<ID>()

    // WAL for durability
    private val wal: WriteAheadLog<ID>

    // Snapshot manager
    private val snapshotManager: SnapshotManager<ID>

    // Scheduler for periodic snapshots
    private val scheduler: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()

    private val lock = ReentrantReadWriteLock()

    init {
        val dataDir = config.dataDir.resolve("collections")
        dataDir.toFile().mkdirs()

        val walPath = dataDir.resolve("$collectionName.wal")
        val snapshotPath = dataDir.resolve("$collectionName.snapshot")

        wal = WriteAheadLog(walPath, config.encryptionKey, idSerializer, idDeserializer)
        snapshotManager = SnapshotManager(snapshotPath, config.encryptionKey, idSerializer, idDeserializer)

        // Recover from snapshot + WAL
        recover()

        // Schedule periodic snapshots
        scheduler.scheduleAtFixedRate(
            { checkAndSnapshot() },
            config.snapshotIntervalMs,
            config.snapshotIntervalMs,
            TimeUnit.MILLISECONDS
        )

        logger.info { "Initialized file-backed storage for collection '$collectionName'" }
    }

    override fun upsert(id: ID, document: ObjectNode) = lock.write {
        // Write to WAL first
        val existing = memoryStorage.findById(id)
        if (existing == null) {
            wal.appendInsert(id, document)
        } else {
            wal.appendUpdate(id, document)
        }

        // Then update memory
        memoryStorage.upsert(id, document)

        // Check if snapshot is needed
        checkAndSnapshot()
    }

    override fun findById(id: ID): ObjectNode? = lock.read {
        memoryStorage.findById(id)
    }

    override fun updateFields(id: ID, updates: Map<String, Any?>): Boolean = lock.write {
        // Update in memory first
        val updated = memoryStorage.updateFields(id, updates)

        if (updated) {
            // Get the updated document
            val doc = memoryStorage.findById(id)
            if (doc != null) {
                // Write the full updated document to WAL
                wal.appendUpdate(id, doc)
                checkAndSnapshot()
            }
        }

        updated
    }

    override fun delete(id: ID): ObjectNode? = lock.write {
        val deleted = memoryStorage.delete(id)
        if (deleted != null) {
            wal.appendDelete(id)
            checkAndSnapshot()
        }
        deleted
    }

    override fun findAll(): List<Pair<ID, ObjectNode>> = lock.read {
        memoryStorage.findAll()
    }

    override fun findAll(skip: Int, limit: Int): List<Pair<ID, ObjectNode>> = lock.read {
        memoryStorage.findAll(skip, limit)
    }

    override fun count(): Long = memoryStorage.count()

    override fun countMatching(predicate: (ObjectNode) -> Boolean): Long = lock.read {
        memoryStorage.countMatching(predicate)
    }

    override fun exists(id: ID): Boolean = memoryStorage.exists(id)

    override fun compact() = lock.write {
        createSnapshot()
    }

    override fun stats(): StorageStats = lock.read {
        val memStats = memoryStorage.stats()
        StorageStats(
            documentCount = memStats.documentCount,
            storageBytes = memStats.storageBytes + snapshotManager.size(),
            walBytes = wal.size(),
            lastSnapshotTime = snapshotManager.lastModified()?.toEpochMilli()
        )
    }

    override fun close() {
        logger.info { "Closing file-backed storage for '$collectionName'" }

        // Stop scheduler
        scheduler.shutdown()
        try {
            scheduler.awaitTermination(5, TimeUnit.SECONDS)
        } catch (e: InterruptedException) {
            scheduler.shutdownNow()
        }

        // Create final snapshot
        try {
            createSnapshot()
        } catch (e: Exception) {
            logger.error(e) { "Failed to create final snapshot" }
        }

        // Close WAL
        wal.close()

        // Close memory storage
        memoryStorage.close()
    }

    /**
     * Recovers data from snapshot and WAL.
     */
    private fun recover() {
        logger.info { "Recovering collection '$collectionName'..." }

        // Load snapshot first
        if (snapshotManager.exists()) {
            val documents = snapshotManager.loadSnapshot()
            for ((id, doc) in documents) {
                memoryStorage.upsert(id, doc)
            }
            logger.info { "Loaded ${documents.size} documents from snapshot" }
        }

        // Replay WAL
        val walEntries = wal.readAll()
        for (entry in walEntries) {
            when (entry) {
                is WALEntry.Insert -> memoryStorage.upsert(entry.id, entry.document)
                is WALEntry.Update -> memoryStorage.upsert(entry.id, entry.document)
                is WALEntry.Delete -> memoryStorage.delete(entry.id)
            }
        }

        if (walEntries.isNotEmpty()) {
            logger.info { "Replayed ${walEntries.size} WAL entries" }
        }

        logger.info { "Recovery complete: ${memoryStorage.count()} documents" }
    }

    /**
     * Checks if a snapshot is needed and creates one if necessary.
     */
    private fun checkAndSnapshot() {
        val walSize = wal.size()
        if (walSize >= config.walMaxBytesBeforeSnapshot) {
            logger.info { "WAL size ($walSize bytes) exceeds threshold, creating snapshot..." }
            createSnapshot()
        }
    }

    /**
     * Creates a snapshot of the current state and clears the WAL.
     */
    private fun createSnapshot() {
        try {
            val documents = memoryStorage.findAll()
            snapshotManager.createSnapshot(documents)
            wal.clear()
            logger.info { "Snapshot created with ${documents.size} documents" }
        } catch (e: Exception) {
            logger.error(e) { "Failed to create snapshot" }
        }
    }
}
