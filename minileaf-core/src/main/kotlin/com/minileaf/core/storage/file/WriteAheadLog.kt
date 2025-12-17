package com.minileaf.core.storage.file

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.minileaf.core.crypto.Encryption
import com.minileaf.core.exception.StorageException
import mu.KotlinLogging
import java.io.File
import java.io.RandomAccessFile
import java.nio.file.Path
import java.time.Instant
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.write

/**
 * Write-Ahead Log implementation.
 * Appends operations to a file for crash recovery.
 * @param ID The type of document identifier
 */
class WriteAheadLog<ID : Any>(
    private val walFile: Path,
    private val encryptionKey: ByteArray? = null,
    private val idSerializer: (ID) -> String,
    private val idDeserializer: (String) -> ID
) {
    private val logger = KotlinLogging.logger {}
    private val lock = ReentrantReadWriteLock()
    private val objectMapper = ObjectMapper().apply { registerKotlinModule() }

    private var raf: RandomAccessFile? = null
    private var currentSize: Long = 0

    init {
        open()
    }

    /**
     * Opens the WAL file.
     */
    private fun open() {
        try {
            walFile.parent?.toFile()?.mkdirs()
            val file = walFile.toFile()
            raf = RandomAccessFile(file, "rw")
            currentSize = file.length()
            logger.info { "Opened WAL: $walFile (size: $currentSize bytes)" }
        } catch (e: Exception) {
            throw StorageException("Failed to open WAL: ${e.message}", e)
        }
    }

    /**
     * Appends an insert operation to the WAL.
     */
    fun appendInsert(id: ID, document: ObjectNode) = lock.write {
        val entry = WALEntry.Insert(
            timestamp = Instant.now(),
            id = id,
            document = document
        )
        appendEntry(entry)
    }

    /**
     * Appends an update operation to the WAL.
     */
    fun appendUpdate(id: ID, document: ObjectNode) = lock.write {
        val entry = WALEntry.Update(
            timestamp = Instant.now(),
            id = id,
            document = document
        )
        appendEntry(entry)
    }

    /**
     * Appends a delete operation to the WAL.
     */
    fun appendDelete(id: ID) = lock.write {
        val entry = WALEntry.Delete(
            timestamp = Instant.now(),
            id = id
        )
        appendEntry(entry)
    }

    /**
     * Appends an entry to the WAL file.
     */
    private fun appendEntry(entry: WALEntry<ID>) {
        try {
            val json = serializeEntry(entry)
            val bytes = (json + "\n").toByteArray(Charsets.UTF_8)

            val finalBytes = if (encryptionKey != null) {
                encrypt(bytes, encryptionKey)
            } else {
                bytes
            }

            raf?.write(finalBytes)
            raf?.fd?.sync() // Ensure durability
            currentSize += finalBytes.size
        } catch (e: Exception) {
            throw StorageException("Failed to append WAL entry: ${e.message}", e)
        }
    }

    /**
     * Reads all entries from the WAL for recovery.
     */
    fun readAll(): List<WALEntry<ID>> {
        val entries = mutableListOf<WALEntry<ID>>()

        try {
            val file = walFile.toFile()
            if (!file.exists()) return emptyList()

            val lines = if (encryptionKey != null) {
                // Decrypt and read
                val encryptedBytes = file.readBytes()
                if (encryptedBytes.isEmpty()) {
                    emptyList()
                } else {
                    val decryptedBytes = decrypt(encryptedBytes, encryptionKey)
                    String(decryptedBytes, Charsets.UTF_8).lines()
                }
            } else {
                file.readLines(Charsets.UTF_8)
            }

            for (line in lines) {
                if (line.isBlank()) continue
                try {
                    val entry = deserializeEntry(line)
                    entries.add(entry)
                } catch (e: Exception) {
                    logger.warn { "Failed to parse WAL entry: ${e.message}" }
                }
            }

            logger.info { "Read ${entries.size} entries from WAL" }
        } catch (e: Exception) {
            throw StorageException("Failed to read WAL: ${e.message}", e)
        }

        return entries
    }

    /**
     * Clears the WAL (typically after a snapshot).
     */
    fun clear() = lock.write {
        try {
            raf?.close()
            walFile.toFile().delete()
            open()
            logger.info { "Cleared WAL" }
        } catch (e: Exception) {
            throw StorageException("Failed to clear WAL: ${e.message}", e)
        }
    }

    /**
     * Returns the current size of the WAL in bytes.
     */
    fun size(): Long = currentSize

    /**
     * Closes the WAL.
     */
    fun close() = lock.write {
        try {
            raf?.close()
            raf = null
        } catch (e: Exception) {
            logger.warn { "Error closing WAL: ${e.message}" }
        }
    }

    /**
     * Serializes a WAL entry to JSON.
     */
    private fun serializeEntry(entry: WALEntry<ID>): String {
        val map = mutableMapOf<String, Any?>()

        when (entry) {
            is WALEntry.Insert -> {
                map["type"] = "insert"
                map["timestamp"] = entry.timestamp.toEpochMilli()
                map["id"] = idSerializer(entry.id)
                map["document"] = entry.document
            }
            is WALEntry.Update -> {
                map["type"] = "update"
                map["timestamp"] = entry.timestamp.toEpochMilli()
                map["id"] = idSerializer(entry.id)
                map["document"] = entry.document
            }
            is WALEntry.Delete -> {
                map["type"] = "delete"
                map["timestamp"] = entry.timestamp.toEpochMilli()
                map["id"] = idSerializer(entry.id)
            }
        }

        return objectMapper.writeValueAsString(map)
    }

    /**
     * Deserializes a WAL entry from JSON.
     */
    private fun deserializeEntry(json: String): WALEntry<ID> {
        @Suppress("UNCHECKED_CAST")
        val map = objectMapper.readValue(json, Map::class.java) as Map<String, Any?>

        val type = map["type"] as String
        val timestamp = Instant.ofEpochMilli((map["timestamp"] as Number).toLong())
        val id = idDeserializer(map["id"] as String)

        return when (type) {
            "insert" -> {
                val document = objectMapper.valueToTree<ObjectNode>(map["document"])
                WALEntry.Insert(timestamp, id, document)
            }
            "update" -> {
                val document = objectMapper.valueToTree<ObjectNode>(map["document"])
                WALEntry.Update(timestamp, id, document)
            }
            "delete" -> WALEntry.Delete(timestamp, id)
            else -> throw StorageException("Unknown WAL entry type: $type")
        }
    }

    /**
     * Encrypts bytes using AES-256-GCM.
     */
    private fun encrypt(data: ByteArray, key: ByteArray): ByteArray {
        return Encryption.encrypt(data, key)
    }

    /**
     * Decrypts bytes using AES-256-GCM.
     */
    private fun decrypt(data: ByteArray, key: ByteArray): ByteArray {
        return Encryption.decrypt(data, key)
    }
}
