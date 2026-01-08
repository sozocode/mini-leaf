package com.minileaf.core.storage.file

import com.minileaf.core.crypto.Encryption
import com.minileaf.core.document.DocumentUtils
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path

class WriteAheadLogEncryptionTest {

    @TempDir
    lateinit var tempDir: Path

    private lateinit var encryptionKey: ByteArray

    @BeforeEach
    fun setup() {
        encryptionKey = Encryption.generateKey()
    }

    @Test
    fun `WAL with encryption should persist and recover multiple entries`() {
        val walPath = tempDir.resolve("test.wal")

        // Create WAL with encryption and add multiple entries
        val wal = WriteAheadLog<String>(
            walFile = walPath,
            encryptionKey = encryptionKey,
            idSerializer = { it },
            idDeserializer = { it }
        )

        val doc1 = DocumentUtils.createDocument().apply { put("name", "Alice") }
        val doc2 = DocumentUtils.createDocument().apply { put("name", "Bob") }
        val doc3 = DocumentUtils.createDocument().apply { put("name", "Charlie") }

        wal.appendInsert("1", doc1)
        wal.appendInsert("2", doc2)
        wal.appendInsert("3", doc3)
        wal.close()

        // Read entries back
        val wal2 = WriteAheadLog<String>(
            walFile = walPath,
            encryptionKey = encryptionKey,
            idSerializer = { it },
            idDeserializer = { it }
        )

        val entries = wal2.readAll()
        wal2.close()

        assertThat(entries).hasSize(3)
        assertThat((entries[0] as WALEntry.Insert).id).isEqualTo("1")
        assertThat((entries[1] as WALEntry.Insert).id).isEqualTo("2")
        assertThat((entries[2] as WALEntry.Insert).id).isEqualTo("3")
    }

    @Test
    fun `WAL without encryption should work correctly`() {
        val walPath = tempDir.resolve("test-no-enc.wal")

        val wal = WriteAheadLog<String>(
            walFile = walPath,
            encryptionKey = null,
            idSerializer = { it },
            idDeserializer = { it }
        )

        val doc1 = DocumentUtils.createDocument().apply { put("name", "Alice") }
        val doc2 = DocumentUtils.createDocument().apply { put("name", "Bob") }

        wal.appendInsert("1", doc1)
        wal.appendInsert("2", doc2)
        wal.close()

        val wal2 = WriteAheadLog<String>(
            walFile = walPath,
            encryptionKey = null,
            idSerializer = { it },
            idDeserializer = { it }
        )

        val entries = wal2.readAll()
        wal2.close()

        assertThat(entries).hasSize(2)
    }
}
