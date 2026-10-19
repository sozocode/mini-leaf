package com.minileaf.core.storage.cached

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.minileaf.core.crypto.Encryption
import com.minileaf.core.exception.StorageException
import mu.KotlinLogging
import java.io.ByteArrayOutputStream
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

/**
 * Thread-safe disk-based document store for high-concurrency applications.
 *
 * Documents are appended to a file with a simple format:
 * [ID_LENGTH:4][ID_BYTES][DOC_LENGTH:4][DOC_BYTES]
 *
 * An in-memory index maps ID -> file offset for fast lookups.
 * Updates append new versions (old versions reclaimed during compaction).
 *
 * Thread-safety guarantees:
 * - Multiple concurrent reads are allowed (using FileChannel position-based reads)
 * - Writes are exclusive (one writer blocks all readers and other writers)
 * - All index lookups and file reads are atomic within the same lock scope
 * - No deadlocks: methods never try to upgrade from read to write lock
 *
 * @param dataFile Path to the data file
 * @param idSerializer Function to serialize ID to string
 * @param idDeserializer Function to deserialize string to ID
 * @param syncOnWrite If true, calls fsync after each write to ensure durability.
 *                    Set to false for better performance at the cost of potential data loss on crash.
 *                    Default is true for data safety.
 * @param encryptionKey Optional 32-byte key for AES-256-GCM encryption. Null = no encryption.
 */
class DiskStore<ID : Comparable<ID>>(
    private val dataFile: Path,
    private val idSerializer: (ID) -> String,
    private val idDeserializer: (String) -> ID,
    private val syncOnWrite: Boolean = true,
    private val encryptionKey: ByteArray? = null
) {
    private val logger = KotlinLogging.logger {}
    private val mapper = ObjectMapper()

    // In-memory index: ID -> file offset
    private val index = ConcurrentHashMap<ID, Long>()

    // Deleted IDs (removed from index but still in file until compaction)
    private val deletedIds = ConcurrentHashMap<ID, Boolean>()

    private val lock = ReentrantReadWriteLock()

    @Volatile  // Ensure visibility across threads after compact() reassigns
    private var raf: RandomAccessFile

    // Maximum allowed lengths to prevent OOM from corrupted data
    private val maxIdLength = 10_000          // 10KB max for ID
    private val maxDocLength = 100 * 1024 * 1024  // 100MB max for document

    // Smallest length prefixes a genuine record can carry. Anything below is framing damage,
    // not a short document: without these floors a run of zero bytes (what a disk-full write or
    // a sparse preallocation leaves behind) parses as an endless series of empty "records".
    //   encrypted: 12-byte IV + 16-byte GCM tag + [ID_LENGTH:4][DOC_LENGTH:4] with nothing in them
    //   plain:     an ID is at least one byte, a document at least "{}"
    private val minEncryptedLength = 12 + 16 + 8
    private val minIdLength = 1
    private val minDocLength = 2

    // Test hook: invoked after a record's length prefix has been written but before its payload,
    // so a test can fail an append halfway and prove the file is rolled back. Never set in
    // production.
    internal var afterPartialAppendHook: (() -> Unit)? = null

    // Records whose framing was intact but whose payload would not decrypt or parse during the
    // last index build. They are skipped, not fatal; compaction drops them.
    @Volatile
    private var corruptEntries: Int = 0

    // Scratch file compaction writes before swapping it over dataFile.
    private val compactTempFile: Path = dataFile.resolveSibling("${dataFile.fileName}.compact")

    // Total entries physically present in the file (live documents, superseded versions and
    // tombstones alike). Seeded by buildIndex(), incremented per append, reset by compact().
    // Drives bloatRatio(), which is how callers decide when compaction is worth doing.
    private val appendCount = AtomicLong(0)

    // False when buildIndex() hit a damaged record and stopped early, meaning some suffix of
    // the file is present on disk but absent from the index. Compaction MUST NOT run in that
    // state: it rewrites the file from the index and would make the loss permanent.
    @Volatile
    private var indexComplete: Boolean = true

    // Offset of the record that stopped the index build, or -1 when the index is complete.
    @Volatile
    private var incompleteTailOffset: Long = -1

    init {
        dataFile.parent?.toFile()?.mkdirs()
        recoverInterruptedCompaction()
        raf = RandomAccessFile(dataFile.toFile(), "rw")

        // Build index by scanning the file
        buildIndex()
    }

    /**
     * Recovers from a compaction that was interrupted before its file swap completed.
     *
     * Older versions swapped by deleting [dataFile] and then renaming the temp file over it,
     * so a crash between those two steps left the collection with no data file and a complete
     * `.compact` file nobody looked at. compact() now swaps atomically, but installs damaged
     * by the old behaviour still need rescuing, and an aborted temp write still leaves litter.
     *
     * Decision rule: the data file wins whenever it holds anything, because compaction only
     * ever swaps in a temp file it has already fsynced. A missing or empty data file next to a
     * temp file means the swap did not finish.
     */
    private fun recoverInterruptedCompaction() {
        if (!Files.exists(compactTempFile)) return

        val dataPresent = Files.exists(dataFile) && Files.size(dataFile) > 0L
        if (dataPresent) {
            logger.warn {
                "Discarding stale compaction temp file ${compactTempFile.fileName} " +
                        "(${Files.size(compactTempFile)} bytes); ${dataFile.fileName} is intact"
            }
            runCatching { Files.deleteIfExists(compactTempFile) }
                .onFailure { logger.error(it) { "Failed to delete stale temp file" } }
            return
        }

        logger.warn {
            "Recovering interrupted compaction: promoting ${compactTempFile.fileName} " +
                    "(${Files.size(compactTempFile)} bytes) to ${dataFile.fileName}"
        }
        atomicMove(compactTempFile, dataFile)
    }

    /**
     * Writes a document to disk and updates the index.
     * If syncOnWrite is true, data is fsynced to disk before returning,
     * ensuring durability even in case of system crash.
     */
    fun write(id: ID, document: ObjectNode) = lock.write {
        val idString = idSerializer(id)
        val idBytes = idString.toByteArray(Charsets.UTF_8)
        val docBytes = mapper.writeValueAsBytes(document)

        // Append to end of file
        raf.seek(raf.length())
        val offset = raf.filePointer

        try {
            appendRecord(idBytes, docBytes)
        } catch (e: Exception) {
            // A failed append (disk full, I/O error) may have left part of a record on disk.
            // Remove it NOW, while we still know where it starts. Left in place, the next append
            // would land after it and every record from here on would be unreachable at the next
            // open, because the index build cannot read past a torn record.
            rollbackPartialAppend(offset, e)
            throw e
        }

        // Update index only after data is safely on disk
        index[id] = offset
        deletedIds.remove(id)
        appendCount.incrementAndGet()
    }

    /**
     * Writes one record at the current file position and, when [syncOnWrite] is set, fsyncs it.
     * The caller has already positioned [raf] at the end of the file.
     */
    private fun appendRecord(idBytes: ByteArray, docBytes: ByteArray) {
        if (encryptionKey != null) {
            // Encrypted format: [TOTAL_LENGTH:4][ENCRYPTED_DATA]
            // where ENCRYPTED_DATA contains encrypted [ID_LENGTH:4][ID_BYTES][DOC_LENGTH:4][DOC_BYTES]
            val plainBytes = buildPlainEntry(idBytes, docBytes)
            val encryptedBytes = Encryption.encrypt(plainBytes, encryptionKey)
            raf.writeInt(encryptedBytes.size)
            afterPartialAppendHook?.invoke()
            raf.write(encryptedBytes)
        } else {
            // Unencrypted format: [ID_LENGTH:4][ID_BYTES][DOC_LENGTH:4][DOC_BYTES]
            raf.writeInt(idBytes.size)
            afterPartialAppendHook?.invoke()
            raf.write(idBytes)
            raf.writeInt(docBytes.size)
            raf.write(docBytes)
        }

        // Ensure data is persisted to disk before updating index
        // This prevents the scenario where index points to data that was never flushed
        if (syncOnWrite) {
            raf.fd.sync()
        }
    }

    /**
     * Truncates the file back to [offset] after an append failed partway.
     *
     * Best effort: on a full disk the truncation itself frees space and normally succeeds. If it
     * does not, the torn record stays on disk and [buildIndex] quarantines it at the next open,
     * so the failure is contained either way.
     */
    private fun rollbackPartialAppend(offset: Long, cause: Exception) {
        try {
            if (raf.length() > offset) {
                raf.setLength(offset)
                if (syncOnWrite) {
                    raf.fd.sync()
                }
            }
            logger.warn(cause) {
                "Append to ${dataFile.fileName} at offset $offset failed; the partial record was removed " +
                        "so the file stays consistent"
            }
        } catch (t: Exception) {
            logger.error(t) {
                "Append to ${dataFile.fileName} at offset $offset failed AND the partial record could not " +
                        "be removed; it will be quarantined when the store is next opened"
            }
        }
    }

    /**
     * Builds a plain entry byte array: [ID_LENGTH:4][ID_BYTES][DOC_LENGTH:4][DOC_BYTES]
     */
    private fun buildPlainEntry(idBytes: ByteArray, docBytes: ByteArray): ByteArray {
        val baos = ByteArrayOutputStream(4 + idBytes.size + 4 + docBytes.size)
        baos.write(intToBytes(idBytes.size))
        baos.write(idBytes)
        baos.write(intToBytes(docBytes.size))
        baos.write(docBytes)
        return baos.toByteArray()
    }

    /**
     * Reads a document from disk by ID.
     * Uses FileChannel with position-based reads for thread-safety.
     *
     * Thread-safe: Multiple threads can call this concurrently.
     * Does NOT attempt to clean up corrupted entries (to avoid deadlock).
     * Use readWithCleanup() if you need automatic corruption cleanup.
     */
    fun read(id: ID): ObjectNode? {
        return lock.read {
            val offset = index[id] ?: return@read null
            readAtOffset(offset)
        }
    }

    /**
     * Reads a document and cleans up if corrupted.
     * WARNING: Do NOT call this from within a lock.read block (deadlock risk).
     * This is safe to call from application code.
     */
    fun readWithCleanup(id: ID): ObjectNode? {
        // First try to read
        val result = read(id)

        // If null but ID exists in index, it might be corrupted
        if (result == null && index.containsKey(id)) {
            // Try to clean up with write lock
            lock.write {
                // Double-check: try reading again under write lock
                val offset = index[id]
                if (offset != null) {
                    val doc = readAtOffset(offset)
                    if (doc == null) {
                        // Confirmed corruption - remove from index
                        logger.error { "Removing corrupted entry from index: $id" }
                        index.remove(id)
                        deletedIds[id] = true
                    }
                }
            }
        }

        return result
    }

    /**
     * Internal read at offset using FileChannel for thread-safe position-based reads.
     * Must be called while holding at least a read lock.
     */
    private fun readAtOffset(offset: Long): ObjectNode? {
        return try {
            val channel = raf.channel

            if (encryptionKey != null) {
                // Encrypted format: [TOTAL_LENGTH:4][ENCRYPTED_DATA]
                val lengthBuffer = ByteBuffer.allocate(4)
                val lengthRead = channel.read(lengthBuffer, offset)
                if (lengthRead < 4) {
                    logger.warn { "Failed to read encrypted length at offset $offset" }
                    return null
                }
                lengthBuffer.flip()
                val encryptedLength = lengthBuffer.int

                if (encryptedLength < 0 || encryptedLength > maxDocLength + maxIdLength + 100) {
                    logger.warn { "Invalid encrypted length at offset $offset: $encryptedLength" }
                    return null
                }

                val encryptedBuffer = ByteBuffer.allocate(encryptedLength)
                val encryptedRead = channel.read(encryptedBuffer, offset + 4)
                if (encryptedRead < encryptedLength) {
                    logger.warn { "Failed to read encrypted data at offset $offset" }
                    return null
                }
                encryptedBuffer.flip()
                val encryptedBytes = ByteArray(encryptedLength)
                encryptedBuffer.get(encryptedBytes)

                val decryptedBytes = Encryption.decrypt(encryptedBytes, encryptionKey)
                return parseDecryptedEntry(decryptedBytes)
            } else {
                // Unencrypted format: [ID_LENGTH:4][ID_BYTES][DOC_LENGTH:4][DOC_BYTES]
                // Read ID length (4 bytes)
                val idLengthBuffer = ByteBuffer.allocate(4)
                val idLengthRead = channel.read(idLengthBuffer, offset)
                if (idLengthRead < 4) {
                    logger.warn { "Failed to read ID length at offset $offset" }
                    return null
                }
                idLengthBuffer.flip()
                val idLength = idLengthBuffer.int

                // Sanity check: idLength should be reasonable
                if (idLength < 0 || idLength > maxIdLength) {
                    logger.warn { "Invalid ID length at offset $offset: $idLength" }
                    return null
                }

                // Read doc length (4 bytes) - skip ID bytes
                val docLengthBuffer = ByteBuffer.allocate(4)
                val docLengthRead = channel.read(docLengthBuffer, offset + 4 + idLength)
                if (docLengthRead < 4) {
                    logger.warn { "Failed to read doc length at offset $offset" }
                    return null
                }
                docLengthBuffer.flip()
                val docLength = docLengthBuffer.int

                // Sanity check: docLength should be reasonable
                if (docLength < 0 || docLength > maxDocLength) {
                    logger.warn { "Invalid document length at offset $offset: $docLength" }
                    return null
                }

                // Read document bytes
                val docBuffer = ByteBuffer.allocate(docLength)
                val docRead = channel.read(docBuffer, offset + 4 + idLength + 4)
                if (docRead < docLength) {
                    logger.warn { "Failed to read document bytes at offset $offset: expected $docLength, got $docRead" }
                    return null
                }
                docBuffer.flip()
                val docBytes = ByteArray(docLength)
                docBuffer.get(docBytes)

                mapper.readTree(docBytes) as ObjectNode
            }
        } catch (e: Exception) {
            logger.error(e) { "Failed to read document at offset $offset" }
            null
        }
    }

    /**
     * Parses a decrypted entry byte array to extract the document.
     */
    private fun parseDecryptedEntry(decryptedBytes: ByteArray): ObjectNode? {
        if (decryptedBytes.size < 8) return null

        val idLength = bytesToInt(decryptedBytes.copyOfRange(0, 4))
        if (idLength < 0 || idLength > maxIdLength || 4 + idLength + 4 > decryptedBytes.size) return null

        val docLength = bytesToInt(decryptedBytes.copyOfRange(4 + idLength, 4 + idLength + 4))
        if (docLength < 0 || docLength > maxDocLength || 4 + idLength + 4 + docLength > decryptedBytes.size) return null

        val docBytes = decryptedBytes.copyOfRange(4 + idLength + 4, 4 + idLength + 4 + docLength)
        return mapper.readTree(docBytes) as ObjectNode
    }

    /**
     * Marks a document as deleted (removes from index).
     * Writes a deletion marker to disk for persistence across restarts.
     * Actual removal happens during compaction.
     */
    fun delete(id: ID): Boolean = lock.write {
        val offset = index[id]
        if (offset == null) {
            return@write false
        }

        // Write deletion marker FIRST, before modifying index
        // This ensures consistency: if we crash after writing marker but before
        // updating index, buildIndex() will see the marker on restart
        val idString = idSerializer(id)
        val idBytes = idString.toByteArray(Charsets.UTF_8)
        val deletionMarker = "{}".toByteArray(Charsets.UTF_8)

        // Append to end of file
        raf.seek(raf.length())
        val markerOffset = raf.filePointer

        try {
            // Same framing as a document; the empty object is what marks it as a tombstone.
            appendRecord(idBytes, deletionMarker)
        } catch (e: Exception) {
            logger.error(e) { "Failed to write deletion marker for ID=$id" }
            rollbackPartialAppend(markerOffset, e)
            // Don't modify index if disk write failed - maintain consistency
            return@write false
        }

        // Only update in-memory state after successful disk write
        index.remove(id)
        deletedIds[id] = true
        appendCount.incrementAndGet()
        true
    }

    /**
     * Checks if a document exists.
     * Note: May return stale result if compact() is running concurrently.
     * For strong consistency, use existsConsistent().
     */
    fun exists(id: ID): Boolean = index.containsKey(id)

    /**
     * Checks if a document exists with strong consistency.
     */
    fun existsConsistent(id: ID): Boolean = lock.read { index.containsKey(id) }

    /**
     * Returns all document IDs.
     * Note: May return stale result if compact() is running concurrently.
     */
    fun allIds(): Set<ID> = index.keys.toSet()

    /**
     * Returns all document IDs with strong consistency.
     */
    fun allIdsConsistent(): Set<ID> = lock.read { index.keys.toSet() }

    /**
     * Returns the number of documents.
     * Note: May return stale result if compact() is running concurrently.
     */
    fun count(): Long = index.size.toLong()

    /**
     * Returns the number of documents with strong consistency.
     */
    fun countConsistent(): Long = lock.read { index.size.toLong() }

    /**
     * Returns all documents (ID, document) pairs.
     * WARNING: Loads all documents into memory!
     *
     * Thread-safe: Uses a single read lock for the entire operation.
     * Corrupted entries are skipped but NOT automatically removed (to avoid deadlock).
     */
    fun readAll(): List<Pair<ID, ObjectNode>> {
        return lock.read {
            val results = mutableListOf<Pair<ID, ObjectNode>>()

            for ((id, offset) in index) {
                val doc = readAtOffset(offset)
                if (doc != null) {
                    results.add(id to doc)
                } else {
                    logger.warn { "Skipping unreadable document: $id at offset $offset" }
                }
            }

            results
        }
    }

    /**
     * Returns all documents and cleans up any corrupted entries.
     * WARNING: This acquires a write lock, blocking all other operations.
     */
    fun readAllWithCleanup(): List<Pair<ID, ObjectNode>> {
        return lock.write {
            val results = mutableListOf<Pair<ID, ObjectNode>>()
            val corruptedIds = mutableListOf<ID>()

            for ((id, offset) in index) {
                val doc = readAtOffset(offset)
                if (doc != null) {
                    results.add(id to doc)
                } else {
                    logger.warn { "Found corrupted document: $id at offset $offset" }
                    corruptedIds.add(id)
                }
            }

            // Remove corrupted entries
            if (corruptedIds.isNotEmpty()) {
                logger.error { "Removing ${corruptedIds.size} corrupted entries from index: $corruptedIds" }
                corruptedIds.forEach { id ->
                    index.remove(id)
                    deletedIds[id] = true
                }
            }

            results
        }
    }

    /**
     * Streams all documents and applies an action.
     * Thread-safe: Uses a single read lock for the entire operation.
     */
    fun forEach(action: (ID, ObjectNode) -> Unit) = lock.read {
        for ((id, offset) in index) {
            val doc = readAtOffset(offset)
            if (doc != null) {
                action(id, doc)
            }
        }
    }

    /**
     * Returns the file size in bytes.
     */
    fun fileSize(): Long = lock.read {
        raf.length()
    }

    /**
     * Ratio of physical entries in the file to live documents.
     *
     * 1.0 immediately after compaction; grows with every update and delete because those append
     * rather than overwrite. 4.0 means three quarters of the file is reclaimable.
     *
     * Returns 0.0 for an empty file, and [Double.MAX_VALUE] when every document has been
     * deleted but tombstones remain (all of the file is reclaimable).
     */
    fun bloatRatio(): Double {
        val entries = appendCount.get()
        if (entries <= 0L) return 0.0
        val live = index.size
        if (live == 0) return Double.MAX_VALUE
        return entries.toDouble() / live
    }

    /**
     * True when the last index build read the whole file successfully.
     *
     * False means a damaged record stopped the scan and some suffix of the file is unreachable.
     * Reads and writes still work; [compact] refuses to run. See [incompleteTailOffset].
     */
    fun isIndexComplete(): Boolean = indexComplete

    /** Offset of the record that stopped the index build, or -1 when the index is complete. */
    fun incompleteTailOffset(): Long = incompleteTailOffset

    /**
     * Records the last index build skipped because their payload was corrupt although their
     * framing was intact. They still occupy file space; [compact] removes them.
     */
    fun corruptEntries(): Int = corruptEntries

    /**
     * Moves [source] onto [target] as a single filesystem operation, so a crash leaves either
     * the old file or the new one, never neither.
     *
     * ATOMIC_MOVE is the guarantee we actually need; it holds for a rename within one
     * filesystem, which is always the case here because the temp file is a sibling. A few
     * filesystems reject the flag, so fall back to a plain replace — still far better than
     * delete-then-rename, since the window shrinks to the rename itself.
     */
    private fun atomicMove(source: Path, target: Path) {
        try {
            Files.move(
                source,
                target,
                StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.ATOMIC_MOVE
            )
        } catch (e: AtomicMoveNotSupportedException) {
            logger.warn(e) { "Atomic move unsupported for ${target.fileName}; falling back to replace" }
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING)
        }
    }

    /**
     * Compacts the file by removing deleted documents.
     * Creates a new file with only active documents.
     *
     * Thread-safe: Acquires exclusive write lock, blocking all other operations.
     */
    fun compact() = lock.write {
        // Refuse to compact an index we know is missing part of the file. Compaction rewrites
        // the file from the index, so running here would permanently delete every record after
        // the damaged one — turning recoverable corruption into data loss.
        if (!indexComplete) {
            throw StorageException(
                "Refusing to compact '${dataFile.fileName}': the index is incomplete because a " +
                        "damaged record at offset $incompleteTailOffset stopped the index build. " +
                        "Compacting now would permanently discard every record after that offset. " +
                        "Repair or restore the file first."
            )
        }

        logger.info { "Compacting disk store: ${index.size} active, ${deletedIds.size} deleted" }

        val tempFile = compactTempFile
        val tempRaf = RandomAccessFile(tempFile.toFile(), "rw")

        try {
            val newIndex = mutableMapOf<ID, Long>()

            // Copy active documents to new file
            for ((id, offset) in index) {
                val doc = readDocumentAtOffsetForCompaction(offset)
                if (doc != null) {
                    val idString = idSerializer(id)
                    val idBytes = idString.toByteArray(Charsets.UTF_8)
                    val docBytes = mapper.writeValueAsBytes(doc)

                    val newOffset = tempRaf.filePointer

                    if (encryptionKey != null) {
                        // Encrypted format
                        val plainBytes = buildPlainEntry(idBytes, docBytes)
                        val encryptedBytes = Encryption.encrypt(plainBytes, encryptionKey)
                        tempRaf.writeInt(encryptedBytes.size)
                        tempRaf.write(encryptedBytes)
                    } else {
                        // Unencrypted format
                        tempRaf.writeInt(idBytes.size)
                        tempRaf.write(idBytes)
                        tempRaf.writeInt(docBytes.size)
                        tempRaf.write(docBytes)
                    }

                    newIndex[id] = newOffset
                } else {
                    logger.warn { "Skipping unreadable document during compaction: $id" }
                }
            }

            // CRITICAL: Ensure all data is flushed to disk before closing/renaming
            tempRaf.fd.sync()
            tempRaf.close()

            // Close old file
            raf.close()

            // Replace old file with new. This MUST be a single atomic operation: the previous
            // delete-then-rename left a window where a crash destroyed the collection outright,
            // because nothing on the reopen path looked for the orphaned temp file.
            atomicMove(tempFile, dataFile)

            // Reopen new file
            raf = RandomAccessFile(dataFile.toFile(), "rw")

            // Update index atomically
            index.clear()
            index.putAll(newIndex)
            deletedIds.clear()

            // The file now holds exactly one record per live document.
            appendCount.set(index.size.toLong())
            corruptEntries = 0

            logger.info { "Compaction complete: ${index.size} documents" }
        } catch (e: Exception) {
            logger.error(e) { "Compaction failed" }
            try { tempRaf.close() } catch (_: Exception) {}
            try { tempFile.toFile().delete() } catch (_: Exception) {}
            throw e
        }
    }

    /**
     * Internal helper to read document at a specific offset using RandomAccessFile.
     * Used during compaction when we already hold the write lock.
     * Includes sanity checks to prevent OOM from corrupted data.
     */
    private fun readDocumentAtOffsetForCompaction(offset: Long): ObjectNode? {
        return try {
            raf.seek(offset)

            if (encryptionKey != null) {
                // Encrypted format: [TOTAL_LENGTH:4][ENCRYPTED_DATA]
                val encryptedLength = raf.readInt()
                if (encryptedLength < 0 || encryptedLength > maxDocLength + maxIdLength + 100) {
                    logger.warn { "Invalid encrypted length during compaction at offset $offset: $encryptedLength" }
                    return null
                }

                val encryptedBytes = ByteArray(encryptedLength)
                raf.readFully(encryptedBytes)

                val decryptedBytes = Encryption.decrypt(encryptedBytes, encryptionKey)
                return parseDecryptedEntry(decryptedBytes)
            } else {
                // Unencrypted format
                val idLength = raf.readInt()
                if (idLength < 0 || idLength > maxIdLength) {
                    logger.warn { "Invalid ID length during compaction at offset $offset: $idLength" }
                    return null
                }

                raf.skipBytes(idLength)

                val docLength = raf.readInt()
                if (docLength < 0 || docLength > maxDocLength) {
                    logger.warn { "Invalid doc length during compaction at offset $offset: $docLength" }
                    return null
                }

                val docBytes = ByteArray(docLength)
                raf.readFully(docBytes)
                mapper.readTree(docBytes) as ObjectNode
            }
        } catch (e: Exception) {
            logger.warn { "Failed to read document at offset $offset during compaction: ${e.message}" }
            null
        }
    }

    /**
     * Closes the disk store.
     * Thread-safe: Acquires exclusive write lock.
     */
    fun close() = lock.write {
        raf.close()
    }

    /**
     * Builds the in-memory index by scanning the data file.
     * Uses a special marker document to track deletions across restarts.
     * Called only during initialization (no concurrent access).
     *
     * Damage is handled in two tiers, because the two kinds are not equally recoverable:
     *
     * - **Corrupt payload, intact framing.** The length prefix is sane and the bytes are all
     *   there, but they will not decrypt (GCM authentication fails) or parse. The record is
     *   skipped and the scan continues at the next one. Nothing after it is affected.
     * - **Lost framing.** The length prefix is impossible, or the record runs past the end of the
     *   file (a torn write from a crash or a full disk). Nothing after this point can be located,
     *   so the unreadable tail is copied to a `.damaged-<timestamp>` sidecar and the file is
     *   truncated at the damage. That restores a consistent file: every later append lands at a
     *   readable boundary instead of behind a wall the next index build cannot see past, which
     *   is how one torn record used to silently lose every write made after it.
     *
     * Only if the sidecar cannot be written does the store fall back to the old behaviour: index
     * what is readable, leave the file alone, and refuse to compact (see [isIndexComplete]).
     */
    private fun buildIndex() {
        // A fresh or emptied file is complete by definition; reset so a reopen after compaction
        // never inherits a stale incompleteness flag.
        indexComplete = true
        incompleteTailOffset = -1
        appendCount.set(0)
        corruptEntries = 0

        val fileLength = raf.length()
        if (fileLength == 0L) {
            return
        }

        val startedAt = System.nanoTime()
        logger.info { "Building index from disk file: ${dataFile.fileName} ($fileLength bytes)" }
        raf.seek(0)

        // Track all IDs and their offsets in order
        val allEntries = mutableMapOf<ID, Long>()
        val deletionMarkers = mutableSetOf<ID>()

        var count = 0
        var skipped = 0

        // Offset at which framing was lost, or -1 if the whole file was walked.
        var damagedAt = -1L
        var damageReason = ""

        scan@ while (raf.filePointer < fileLength) {
            val offset = raf.filePointer

            when (val outcome = readEntryForIndex(offset, fileLength)) {
                is IndexEntry.Record -> {
                    val id = outcome.id
                    val docBytes = outcome.docBytes
                    // Check if this is a deletion marker (empty JSON object: {})
                    if (docBytes.size == 2 && docBytes[0] == '{'.code.toByte() && docBytes[1] == '}'.code.toByte()) {
                        deletionMarkers.add(id)
                        allEntries.remove(id) // Remove from index if it was there
                    } else {
                        allEntries[id] = offset
                        deletionMarkers.remove(id) // Clear any previous deletion marker
                    }
                    count++
                }
                is IndexEntry.CorruptPayload -> {
                    skipped++
                    if (skipped <= MAX_LOGGED_CORRUPT_ENTRIES) {
                        logger.warn(outcome.cause) {
                            "Skipping corrupt record at offset $offset in ${dataFile.fileName} " +
                                    "(${outcome.length} bytes, framing intact): ${outcome.reason}"
                        }
                    }
                }
                is IndexEntry.FramingLost -> {
                    damagedAt = offset
                    damageReason = outcome.reason
                    break@scan
                }
            }
        }

        // Populate the index with only non-deleted documents
        index.clear()
        index.putAll(allEntries)

        // Populate deletedIds with marked deletions
        deletedIds.clear()
        deletionMarkers.forEach { id ->
            deletedIds[id] = true
        }

        // Skipped records are still bytes in the file, so they count towards bloat.
        appendCount.set((count + skipped).toLong())
        corruptEntries = skipped

        if (damagedAt >= 0) {
            quarantineDamagedTail(damagedAt, fileLength, damageReason)
        }

        val elapsedMs = (System.nanoTime() - startedAt) / 1_000_000
        logger.info {
            "Index built: ${index.size} active documents, ${deletedIds.size} deleted, from $count entries " +
                    "(skipped: $skipped) in ${dataFile.fileName} in $elapsedMs ms"
        }
    }

    /** One step of the index scan. On [Record] and [CorruptPayload] the file pointer sits at the next record. */
    private sealed class IndexEntry<out ID> {
        class Record<ID>(val id: ID, val docBytes: ByteArray) : IndexEntry<ID>()
        class CorruptPayload(val length: Int, val reason: String, val cause: Throwable?) : IndexEntry<Nothing>()
        class FramingLost(val reason: String) : IndexEntry<Nothing>()
    }

    /**
     * Reads the record starting at [offset] for the index build.
     *
     * Every framing check happens BEFORE any payload is read, against [fileLength], so a torn
     * record is reported as [IndexEntry.FramingLost] with the pointer still at [offset] rather
     * than surfacing as an EOFException halfway through.
     */
    private fun readEntryForIndex(offset: Long, fileLength: Long): IndexEntry<ID> {
        if (fileLength - offset < 4) {
            return IndexEntry.FramingLost("${fileLength - offset} trailing byte(s), too short for a length prefix")
        }

        if (encryptionKey != null) {
            // Encrypted format: [TOTAL_LENGTH:4][ENCRYPTED_DATA]
            val encryptedLength = raf.readInt()
            if (encryptedLength < minEncryptedLength || encryptedLength > maxDocLength + maxIdLength + 100) {
                return IndexEntry.FramingLost("invalid encrypted length $encryptedLength")
            }
            if (offset + 4 + encryptedLength > fileLength) {
                return IndexEntry.FramingLost(
                    "record of $encryptedLength bytes runs ${offset + 4 + encryptedLength - fileLength} bytes " +
                            "past the end of the file (torn write)"
                )
            }

            val encryptedBytes = ByteArray(encryptedLength)
            raf.readFully(encryptedBytes)

            return try {
                val decryptedBytes = Encryption.decrypt(encryptedBytes, encryptionKey)
                val parsed = parseDecryptedEntryWithId(decryptedBytes)
                    ?: return IndexEntry.CorruptPayload(encryptedLength, "decrypted entry has an invalid layout", null)
                IndexEntry.Record(parsed.first, parsed.second)
            } catch (e: Exception) {
                // GCM authentication failed, or the ID would not deserialize. The framing was
                // consistent, so the next record is exactly where the length prefix says.
                IndexEntry.CorruptPayload(encryptedLength, e.message ?: e.javaClass.simpleName, e)
            }
        }

        // Unencrypted format: [ID_LENGTH:4][ID_BYTES][DOC_LENGTH:4][DOC_BYTES]
        val idLength = raf.readInt()
        if (idLength < minIdLength || idLength > maxIdLength) {
            return IndexEntry.FramingLost("invalid ID length $idLength")
        }
        if (offset + 4 + idLength + 4 > fileLength) {
            return IndexEntry.FramingLost("ID of $idLength bytes runs past the end of the file (torn write)")
        }

        val idBytes = ByteArray(idLength)
        raf.readFully(idBytes)

        val docLength = raf.readInt()
        if (docLength < minDocLength || docLength > maxDocLength) {
            return IndexEntry.FramingLost("invalid document length $docLength")
        }
        val recordEnd = offset + 4 + idLength + 4 + docLength
        if (recordEnd > fileLength) {
            return IndexEntry.FramingLost(
                "document of $docLength bytes runs ${recordEnd - fileLength} bytes past the end of the file (torn write)"
            )
        }

        val docBytes = ByteArray(docLength)
        raf.readFully(docBytes)

        return try {
            IndexEntry.Record(idDeserializer(String(idBytes, Charsets.UTF_8)), docBytes)
        } catch (e: Exception) {
            IndexEntry.CorruptPayload(4 + idLength + 4 + docLength, "ID would not deserialize: ${e.message}", e)
        }
    }

    /**
     * Moves the unreadable tail of the file, from [damagedAt] to [fileLength], into a sidecar
     * file and truncates the data file at [damagedAt].
     *
     * The bytes are preserved, never deleted: a future repair tool, or a human with a hex editor,
     * can still get at whatever records the tail contained. What changes is that the data file
     * is consistent again, so appends made from now on are readable at the next open.
     *
     * If the sidecar cannot be written, the file is left untouched and the store runs with an
     * incomplete index, exactly as before this recovery existed.
     */
    private fun quarantineDamagedTail(damagedAt: Long, fileLength: Long, reason: String) {
        val tailBytes = fileLength - damagedAt
        val stamp = java.time.LocalDateTime.now()
            .format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss-SSS"))
        val sidecar = dataFile.resolveSibling("${dataFile.fileName}.damaged-$stamp")

        try {
            FileChannel.open(
                sidecar,
                java.nio.file.StandardOpenOption.CREATE_NEW,
                java.nio.file.StandardOpenOption.WRITE
            ).use { out ->
                var position = damagedAt
                while (position < fileLength) {
                    val moved = raf.channel.transferTo(position, fileLength - position, out)
                    if (moved <= 0L) throw java.io.IOException("transferTo made no progress at $position")
                    position += moved
                }
                out.force(true)
            }
            val written = Files.size(sidecar)
            if (written != tailBytes) {
                throw java.io.IOException("sidecar holds $written bytes, expected $tailBytes")
            }

            raf.setLength(damagedAt)
            raf.fd.sync()

            logger.error {
                "Recovered ${dataFile.fileName} from a damaged record at offset $damagedAt ($reason): " +
                        "$tailBytes unreadable trailing byte(s) were moved to ${sidecar.fileName} and the " +
                        "data file was truncated at the damage. ${index.size} documents are indexed. " +
                        "Nothing was deleted; the sidecar can be inspected or removed by hand."
            }
        } catch (e: Exception) {
            runCatching { Files.deleteIfExists(sidecar) }
            indexComplete = false
            incompleteTailOffset = damagedAt
            logger.error(e) {
                "Index for ${dataFile.fileName} is INCOMPLETE: a damaged record at offset $damagedAt ($reason) " +
                        "stopped the scan $tailBytes bytes before the end of the file, and the tail could not " +
                        "be quarantined. ${index.size} documents are readable; any records after that offset " +
                        "are not. Compaction is disabled for this collection until the file is repaired."
            }
        }
    }

    /**
     * Parses a decrypted entry to extract both ID and document bytes.
     */
    private fun parseDecryptedEntryWithId(decryptedBytes: ByteArray): Pair<ID, ByteArray>? {
        if (decryptedBytes.size < 8) return null

        val idLength = bytesToInt(decryptedBytes.copyOfRange(0, 4))
        if (idLength < 0 || idLength > maxIdLength || 4 + idLength + 4 > decryptedBytes.size) return null

        val idBytes = decryptedBytes.copyOfRange(4, 4 + idLength)
        val idString = String(idBytes, Charsets.UTF_8)
        val id = idDeserializer(idString)

        val docLength = bytesToInt(decryptedBytes.copyOfRange(4 + idLength, 4 + idLength + 4))
        if (docLength < 0 || docLength > maxDocLength || 4 + idLength + 4 + docLength > decryptedBytes.size) return null

        val docBytes = decryptedBytes.copyOfRange(4 + idLength + 4, 4 + idLength + 4 + docLength)
        return id to docBytes
    }

    /**
     * Converts an integer to a 4-byte array (big-endian).
     */
    private fun intToBytes(value: Int): ByteArray {
        return byteArrayOf(
            (value shr 24).toByte(),
            (value shr 16).toByte(),
            (value shr 8).toByte(),
            value.toByte()
        )
    }

    /**
     * Converts a 4-byte array (big-endian) to an integer.
     */
    private fun bytesToInt(bytes: ByteArray): Int {
        require(bytes.size == 4) { "Expected 4 bytes" }
        return ((bytes[0].toInt() and 0xFF) shl 24) or
               ((bytes[1].toInt() and 0xFF) shl 16) or
               ((bytes[2].toInt() and 0xFF) shl 8) or
               (bytes[3].toInt() and 0xFF)
    }

    private companion object {
        /** Beyond this many corrupt records in one scan, only the summary line is logged. */
        const val MAX_LOGGED_CORRUPT_ENTRIES = 10
    }
}
