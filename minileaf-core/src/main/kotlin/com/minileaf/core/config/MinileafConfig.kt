package com.minileaf.core.config

import java.nio.file.Path
import java.nio.file.Paths

/**
 * Configuration for Minileaf.
 */
data class MinileafConfig(
    /**
     * Directory where data files are stored.
     */
    val dataDir: Path = Paths.get("./minileaf-data"),

    /**
     * Encryption key for AES-256-GCM (32 bytes). Null = no encryption.
     */
    val encryptionKey: ByteArray? = null,

    /**
     * Interval in milliseconds for auto-flushing buffers.
     */
    val autosaveIntervalMs: Long = 5_000,

    /**
     * Interval in milliseconds for creating snapshots.
     */
    val snapshotIntervalMs: Long = 60_000,

    /**
     * Max WAL size in bytes before forcing a snapshot.
     */
    val walMaxBytesBeforeSnapshot: Long = 64L * 1024 * 1024, // 64 MB

    /**
     * If true, run entirely in memory (no persistence).
     */
    val memoryOnly: Boolean = false,

    /**
     * Cache size for cached storage mode (number of documents to keep in memory).
     * If null, uses FileBackedStorageEngine (all documents in memory).
     * If set, uses CachedStorageEngine (bounded memory with LRU cache).
     * Recommended for large datasets: 10000-100000 depending on available RAM.
     */
    val cacheSize: Int? = null,

    /**
     * If true, build indexes in the background.
     */
    val backgroundIndexBuild: Boolean = true,

    /**
     * If true, calls fsync after each write to ensure data durability.
     * This prevents data loss if the process crashes before the OS flushes buffers.
     * Set to false for better write performance at the cost of potential data loss on crash.
     * Default is true for data safety.
     */
    val syncOnWrite: Boolean = true,

    /**
     * Maximum document size in bytes.
     */
    val maxDocumentSize: Long = 16L * 1024 * 1024 // 16 MB
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as MinileafConfig

        if (dataDir != other.dataDir) return false
        if (encryptionKey != null) {
            if (other.encryptionKey == null) return false
            if (!encryptionKey.contentEquals(other.encryptionKey)) return false
        } else if (other.encryptionKey != null) return false
        if (autosaveIntervalMs != other.autosaveIntervalMs) return false
        if (snapshotIntervalMs != other.snapshotIntervalMs) return false
        if (walMaxBytesBeforeSnapshot != other.walMaxBytesBeforeSnapshot) return false
        if (memoryOnly != other.memoryOnly) return false
        if (cacheSize != other.cacheSize) return false
        if (backgroundIndexBuild != other.backgroundIndexBuild) return false
        if (syncOnWrite != other.syncOnWrite) return false
        if (maxDocumentSize != other.maxDocumentSize) return false

        return true
    }

    override fun hashCode(): Int {
        var result = dataDir.hashCode()
        result = 31 * result + (encryptionKey?.contentHashCode() ?: 0)
        result = 31 * result + autosaveIntervalMs.hashCode()
        result = 31 * result + snapshotIntervalMs.hashCode()
        result = 31 * result + walMaxBytesBeforeSnapshot.hashCode()
        result = 31 * result + memoryOnly.hashCode()
        result = 31 * result + (cacheSize ?: 0)
        result = 31 * result + backgroundIndexBuild.hashCode()
        result = 31 * result + syncOnWrite.hashCode()
        result = 31 * result + maxDocumentSize.hashCode()
        return result
    }
}
