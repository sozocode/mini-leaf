package com.minileaf.core.storage.cached

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.minileaf.core.crypto.Encryption
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path

class DiskStoreEncryptionTest {

    @TempDir
    lateinit var tempDir: Path

    private val mapper = ObjectMapper()
    private lateinit var encryptionKey: ByteArray

    @BeforeEach
    fun setup() {
        encryptionKey = Encryption.generateKey()
    }

    private fun createDoc(name: String, age: Int): ObjectNode {
        return mapper.createObjectNode().apply {
            put("name", name)
            put("age", age)
        }
    }

    @Test
    fun `DiskStore with encryption should write and read documents`() {
        val dataFile = tempDir.resolve("encrypted.data")
        val store = DiskStore<String>(
            dataFile = dataFile,
            idSerializer = { it },
            idDeserializer = { it },
            syncOnWrite = true,
            encryptionKey = encryptionKey
        )

        store.write("1", createDoc("Alice", 30))
        store.write("2", createDoc("Bob", 25))

        val doc1 = store.read("1")
        val doc2 = store.read("2")

        assertThat(doc1).isNotNull
        assertThat(doc1!!.get("name").asText()).isEqualTo("Alice")
        assertThat(doc1.get("age").asInt()).isEqualTo(30)

        assertThat(doc2).isNotNull
        assertThat(doc2!!.get("name").asText()).isEqualTo("Bob")

        store.close()
    }

    @Test
    fun `DiskStore with encryption should persist and recover after restart`() {
        val dataFile = tempDir.resolve("persist.data")

        // Write data
        val store1 = DiskStore<String>(
            dataFile = dataFile,
            idSerializer = { it },
            idDeserializer = { it },
            syncOnWrite = true,
            encryptionKey = encryptionKey
        )
        store1.write("1", createDoc("Alice", 30))
        store1.write("2", createDoc("Bob", 25))
        store1.close()

        // Reopen and verify
        val store2 = DiskStore<String>(
            dataFile = dataFile,
            idSerializer = { it },
            idDeserializer = { it },
            syncOnWrite = true,
            encryptionKey = encryptionKey
        )

        assertThat(store2.count()).isEqualTo(2)
        assertThat(store2.read("1")!!.get("name").asText()).isEqualTo("Alice")
        assertThat(store2.read("2")!!.get("name").asText()).isEqualTo("Bob")

        store2.close()
    }

    @Test
    fun `DiskStore with encryption should handle delete correctly`() {
        val dataFile = tempDir.resolve("delete.data")
        val store = DiskStore<String>(
            dataFile = dataFile,
            idSerializer = { it },
            idDeserializer = { it },
            syncOnWrite = true,
            encryptionKey = encryptionKey
        )

        store.write("1", createDoc("Alice", 30))
        store.write("2", createDoc("Bob", 25))
        store.delete("1")

        assertThat(store.read("1")).isNull()
        assertThat(store.read("2")).isNotNull
        assertThat(store.count()).isEqualTo(1)

        store.close()

        // Verify deletion persists across restart
        val store2 = DiskStore<String>(
            dataFile = dataFile,
            idSerializer = { it },
            idDeserializer = { it },
            syncOnWrite = true,
            encryptionKey = encryptionKey
        )

        assertThat(store2.read("1")).isNull()
        assertThat(store2.count()).isEqualTo(1)

        store2.close()
    }

    @Test
    fun `DiskStore encrypted data should not be readable as plaintext`() {
        val dataFile = tempDir.resolve("binary.data")
        val store = DiskStore<String>(
            dataFile = dataFile,
            idSerializer = { it },
            idDeserializer = { it },
            syncOnWrite = true,
            encryptionKey = encryptionKey
        )

        store.write("secret", createDoc("SecretName", 99))
        store.close()

        // Read raw file contents
        val rawBytes = dataFile.toFile().readBytes()
        val rawString = String(rawBytes, Charsets.UTF_8)

        // The name should NOT appear in plaintext
        assertThat(rawString).doesNotContain("SecretName")
        assertThat(rawString).doesNotContain("secret")
    }

    @Test
    fun `DiskStore without encryption should store plaintext`() {
        val dataFile = tempDir.resolve("plaintext.data")
        val store = DiskStore<String>(
            dataFile = dataFile,
            idSerializer = { it },
            idDeserializer = { it },
            syncOnWrite = true,
            encryptionKey = null
        )

        store.write("visible", createDoc("VisibleName", 42))
        store.close()

        // Read raw file contents
        val rawBytes = dataFile.toFile().readBytes()
        val rawString = String(rawBytes, Charsets.UTF_8)

        // The name SHOULD appear in plaintext
        assertThat(rawString).contains("VisibleName")
    }

    @Test
    fun `DiskStore with encryption should compact correctly`() {
        val dataFile = tempDir.resolve("compact.data")
        val store = DiskStore<String>(
            dataFile = dataFile,
            idSerializer = { it },
            idDeserializer = { it },
            syncOnWrite = true,
            encryptionKey = encryptionKey
        )

        // Write and delete to create garbage
        store.write("1", createDoc("Alice", 30))
        store.write("2", createDoc("Bob", 25))
        store.write("3", createDoc("Charlie", 35))
        store.delete("2")

        val sizeBeforeCompact = store.fileSize()
        store.compact()
        val sizeAfterCompact = store.fileSize()

        // File should be smaller after compaction
        assertThat(sizeAfterCompact).isLessThan(sizeBeforeCompact)

        // Data should still be correct
        assertThat(store.count()).isEqualTo(2)
        assertThat(store.read("1")!!.get("name").asText()).isEqualTo("Alice")
        assertThat(store.read("3")!!.get("name").asText()).isEqualTo("Charlie")
        assertThat(store.read("2")).isNull()

        store.close()
    }
}
