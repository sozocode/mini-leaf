package com.minileaf.core.storage.file

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.minileaf.core.crypto.Encryption
import com.minileaf.core.exception.StorageException
import mu.KotlinLogging
import java.nio.file.Path
import java.time.Instant

/**
 * Manages snapshots of the collection data.
 * Snapshots are compact representations of all documents.
 * @param ID The type of document identifier
 */
class SnapshotManager<ID : Any>(
    private val snapshotFile: Path,
    private val encryptionKey: ByteArray? = null,
    private val idSerializer: (ID) -> String,
    private val idDeserializer: (String) -> ID
) {
    private val logger = KotlinLogging.logger {}
    private val objectMapper = ObjectMapper().apply { registerKotlinModule() }

    /**
     * Creates a snapshot from the given documents.
     */
    fun createSnapshot(documents: List<Pair<ID, ObjectNode>>) {
        try {
            snapshotFile.parent?.toFile()?.mkdirs()

            val snapshot = documents.map { (id, doc) ->
                mapOf("_id" to idSerializer(id), "doc" to doc)
            }

            val json = objectMapper.writeValueAsString(snapshot)
            val bytes = json.toByteArray(Charsets.UTF_8)

            val finalBytes = if (encryptionKey != null) {
                encrypt(bytes, encryptionKey)
            } else {
                bytes
            }

            snapshotFile.toFile().writeBytes(finalBytes)
            logger.info { "Created snapshot with ${documents.size} documents (${finalBytes.size} bytes)" }
        } catch (e: Exception) {
            throw StorageException("Failed to create snapshot: ${e.message}", e)
        }
    }

    /**
     * Loads documents from the snapshot.
     */
    fun loadSnapshot(): List<Pair<ID, ObjectNode>> {
        try {
            val file = snapshotFile.toFile()
            if (!file.exists()) {
                logger.info { "No snapshot file found" }
                return emptyList()
            }

            val bytes = file.readBytes()
            if (bytes.isEmpty()) {
                logger.info { "Snapshot file is empty" }
                return emptyList()
            }

            val finalBytes = if (encryptionKey != null) {
                decrypt(bytes, encryptionKey)
            } else {
                bytes
            }

            val json = String(finalBytes, Charsets.UTF_8)

            @Suppress("UNCHECKED_CAST")
            val snapshotList = objectMapper.readValue(json, List::class.java) as List<Map<String, Any>>

            val documents = snapshotList.map { entry ->
                val id = idDeserializer(entry["_id"] as String)
                val doc = objectMapper.valueToTree<ObjectNode>(entry["doc"])
                id to doc
            }

            logger.info { "Loaded ${documents.size} documents from snapshot" }
            return documents
        } catch (e: Exception) {
            throw StorageException("Failed to load snapshot: ${e.message}", e)
        }
    }

    /**
     * Checks if a snapshot exists.
     */
    fun exists(): Boolean = snapshotFile.toFile().exists()

    /**
     * Returns the size of the snapshot file in bytes.
     */
    fun size(): Long = if (exists()) snapshotFile.toFile().length() else 0

    /**
     * Returns the last modified time of the snapshot.
     */
    fun lastModified(): Instant? {
        return if (exists()) {
            Instant.ofEpochMilli(snapshotFile.toFile().lastModified())
        } else {
            null
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
