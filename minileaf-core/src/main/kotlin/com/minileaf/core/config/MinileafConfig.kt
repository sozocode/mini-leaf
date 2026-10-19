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
     *
     * **File-backed engine only** (`cacheSize == null`). The cached engine writes through to disk
     * on every operation and has no buffer to flush, so this value is ignored there.
     */
    val autosaveIntervalMs: Long = 5_000,

    /**
     * Interval in milliseconds for creating snapshots.
     *
     * **File-backed engine only.** The cached engine has no WAL and takes no snapshots; its
     * equivalent housekeeping is compaction, governed by the `autoCompact*` settings below.
     */
    val snapshotIntervalMs: Long = 60_000,

    /**
     * Max WAL size in bytes before forcing a snapshot.
     *
     * **File-backed engine only.** Ignored by the cached engine, which has no WAL.
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
    val maxDocumentSize: Long = 16L * 1024 * 1024, // 16 MB

    /**
     * If true, cached storage compacts its data files automatically in the background.
     *
     * Updates and deletes append rather than overwrite, so without compaction a data file
     * grows without bound — a collection updated on a timer reaches hundreds of megabytes
     * while holding a few kilobytes of live data. Disable only if you drive `compact()`
     * yourself.
     */
    val autoCompactEnabled: Boolean = true,

    /**
     * How often to evaluate collections for automatic compaction.
     */
    val autoCompactIntervalMs: Long = 5L * 60 * 1000, // 5 minutes

    /**
     * Files smaller than this are never auto-compacted, however bloated they look by ratio.
     * Keeps small collections from churning for no useful gain.
     */
    val autoCompactMinBytes: Long = 8L * 1024 * 1024, // 8 MB

    /**
     * Auto-compact once physical entries exceed live documents by this factor.
     * 4.0 means compaction runs when roughly three quarters of the file is reclaimable.
     */
    val autoCompactBloatRatio: Double = 4.0,

    /**
     * If true, opening a persistent store takes an OS-level lock on `<dataDir>/minileaf.lock`
     * and refuses to open while another process holds it.
     *
     * Two processes on one data directory is not a supported configuration, and it does not
     * fail loudly on its own: each appends to the same files with its own private index, so
     * every write one of them makes is invisible to the other until the next open — and
     * compaction in either rewrites the file from an index that is missing the other's records.
     * The classic way to get there is an orphaned previous instance that never exited. Failing
     * fast with the holder's PID in the message is the fix; disable only for tooling that
     * deliberately opens a store read-only beside a live process.
     */
    val lockDataDir: Boolean = true
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
        if (autoCompactEnabled != other.autoCompactEnabled) return false
        if (autoCompactIntervalMs != other.autoCompactIntervalMs) return false
        if (autoCompactMinBytes != other.autoCompactMinBytes) return false
        if (autoCompactBloatRatio != other.autoCompactBloatRatio) return false
        if (lockDataDir != other.lockDataDir) return false

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
        result = 31 * result + autoCompactEnabled.hashCode()
        result = 31 * result + autoCompactIntervalMs.hashCode()
        result = 31 * result + autoCompactMinBytes.hashCode()
        result = 31 * result + autoCompactBloatRatio.hashCode()
        result = 31 * result + lockDataDir.hashCode()
        return result
    }
}
