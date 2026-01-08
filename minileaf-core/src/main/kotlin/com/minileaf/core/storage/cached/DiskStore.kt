package com.minileaf.core.storage.cached

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.minileaf.core.crypto.Encryption
import mu.KotlinLogging
import java.io.ByteArrayOutputStream
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
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

    init {
        dataFile.parent?.toFile()?.mkdirs()
        raf = RandomAccessFile(dataFile.toFile(), "rw")

        // Build index by scanning the file
        buildIndex()
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

        if (encryptionKey != null) {
            // Encrypted format: [TOTAL_LENGTH:4][ENCRYPTED_DATA]
            // where ENCRYPTED_DATA contains encrypted [ID_LENGTH:4][ID_BYTES][DOC_LENGTH:4][DOC_BYTES]
            val plainBytes = buildPlainEntry(idBytes, docBytes)
            val encryptedBytes = Encryption.encrypt(plainBytes, encryptionKey)
            raf.writeInt(encryptedBytes.size)
            raf.write(encryptedBytes)
        } else {
            // Unencrypted format: [ID_LENGTH:4][ID_BYTES][DOC_LENGTH:4][DOC_BYTES]
            raf.writeInt(idBytes.size)
            raf.write(idBytes)
            raf.writeInt(docBytes.size)
            raf.write(docBytes)
        }

        // Ensure data is persisted to disk before updating index
        // This prevents the scenario where index points to data that was never flushed
        if (syncOnWrite) {
            raf.fd.sync()
        }

        // Update index only after data is safely on disk
        index[id] = offset
        deletedIds.remove(id)
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
        try {
            val idString = idSerializer(id)
            val idBytes = idString.toByteArray(Charsets.UTF_8)
            val deletionMarker = "{}".toByteArray(Charsets.UTF_8)

            // Append to end of file
            raf.seek(raf.length())

            if (encryptionKey != null) {
                // Encrypted format
                val plainBytes = buildPlainEntry(idBytes, deletionMarker)
                val encryptedBytes = Encryption.encrypt(plainBytes, encryptionKey)
                raf.writeInt(encryptedBytes.size)
                raf.write(encryptedBytes)
            } else {
                // Unencrypted format: [ID_LENGTH:4][ID_BYTES][DOC_LENGTH:4][DELETION_MARKER]
                raf.writeInt(idBytes.size)
                raf.write(idBytes)
                raf.writeInt(deletionMarker.size)
                raf.write(deletionMarker)
            }

            // Ensure deletion marker is persisted
            if (syncOnWrite) {
                raf.fd.sync()
            }
        } catch (e: Exception) {
            logger.error(e) { "Failed to write deletion marker for ID=$id" }
            // Don't modify index if disk write failed - maintain consistency
            return@write false
        }

        // Only update in-memory state after successful disk write
        index.remove(id)
        deletedIds[id] = true
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
     * Compacts the file by removing deleted documents.
     * Creates a new file with only active documents.
     *
     * Thread-safe: Acquires exclusive write lock, blocking all other operations.
     */
    fun compact() = lock.write {
        logger.info { "Compacting disk store: ${index.size} active, ${deletedIds.size} deleted" }

        val tempFile = dataFile.parent.resolve("${dataFile.fileName}.compact")
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

            // Replace old file with new
            dataFile.toFile().delete()
            if (!tempFile.toFile().renameTo(dataFile.toFile())) {
                throw java.io.IOException("Failed to rename compacted file")
            }

            // Reopen new file
            raf = RandomAccessFile(dataFile.toFile(), "rw")

            // Update index atomically
            index.clear()
            index.putAll(newIndex)
            deletedIds.clear()

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
     */
    private fun buildIndex() {
        if (raf.length() == 0L) {
            return
        }

        logger.info { "Building index from disk file: ${dataFile.fileName}" }
        raf.seek(0)

        // Track all IDs and their offsets in order
        val allEntries = mutableMapOf<ID, Long>()
        val deletionMarkers = mutableSetOf<ID>()

        var count = 0
        var skipped = 0

        while (raf.filePointer < raf.length()) {
            val offset = raf.filePointer

            try {
                if (encryptionKey != null) {
                    // Encrypted format: [TOTAL_LENGTH:4][ENCRYPTED_DATA]
                    val encryptedLength = raf.readInt()
                    if (encryptedLength < 0 || encryptedLength > maxDocLength + maxIdLength + 100) {
                        logger.error { "Invalid encrypted length at offset $offset: $encryptedLength, stopping index build" }
                        break
                    }

                    val encryptedBytes = ByteArray(encryptedLength)
                    raf.readFully(encryptedBytes)

                    val decryptedBytes = Encryption.decrypt(encryptedBytes, encryptionKey)
                    val parsed = parseDecryptedEntryWithId(decryptedBytes)
                    if (parsed != null) {
                        val (id, docBytes) = parsed
                        // Check if this is a deletion marker (empty JSON object: {})
                        if (docBytes.size == 2 && docBytes[0] == '{'.code.toByte() && docBytes[1] == '}'.code.toByte()) {
                            deletionMarkers.add(id)
                            allEntries.remove(id)
                        } else {
                            allEntries[id] = offset
                            deletionMarkers.remove(id)
                        }
                    }
                } else {
                    // Unencrypted format: [ID_LENGTH:4][ID_BYTES][DOC_LENGTH:4][DOC_BYTES]
                    val idLength = raf.readInt()

                    // Sanity check
                    if (idLength < 0 || idLength > maxIdLength) {
                        logger.error { "Invalid ID length at offset $offset: $idLength, stopping index build" }
                        break
                    }

                    val idBytes = ByteArray(idLength)
                    raf.readFully(idBytes)
                    val idString = String(idBytes, Charsets.UTF_8)
                    val id = idDeserializer(idString)

                    val docLength = raf.readInt()

                    // Sanity check
                    if (docLength < 0 || docLength > maxDocLength) {
                        logger.error { "Invalid doc length at offset $offset: $docLength, stopping index build" }
                        break
                    }

                    val docBytes = ByteArray(docLength)
                    raf.readFully(docBytes)

                    // Check if this is a deletion marker (empty JSON object: {})
                    if (docLength == 2 && docBytes[0] == '{'.code.toByte() && docBytes[1] == '}'.code.toByte()) {
                        // This is a deletion marker
                        deletionMarkers.add(id)
                        allEntries.remove(id) // Remove from index if it was there
                    } else {
                        // Normal document - only add if not marked as deleted later
                        allEntries[id] = offset
                        deletionMarkers.remove(id) // Clear any previous deletion marker
                    }
                }

                count++
            } catch (e: Exception) {
                logger.error(e) { "Failed to read entry at offset $offset, stopping index build" }
                skipped++
                break
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

        logger.info { "Index built: ${index.size} active documents, ${deletedIds.size} deleted, from $count entries (skipped: $skipped)" }
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
}
