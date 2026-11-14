package com.minileaf.core.storage.cached

import com.fasterxml.jackson.databind.node.ObjectNode
import com.minileaf.core.crypto.Encryption
import com.minileaf.core.document.DocumentUtils
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.io.IOException
import java.io.RandomAccessFile
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.name

/**
 * What happens to the records written AFTER a damaged one.
 *
 * The index build used to stop at the first record it could not read. Reads of everything
 * before it kept working, so nothing looked broken — but every append from then on landed
 * behind the damage, where no later index build could see it. One torn record from a crash or
 * a full disk, and the store silently dropped every write made for the rest of its life.
 *
 * Two fixes, both pinned here:
 *
 * 1. An append that fails partway rolls the file back, so a torn record never ends up in the
 *    middle of the file in the first place.
 * 2. Damage found at open is classified. A record whose framing is intact but whose payload is
 *    corrupt is skipped. A record whose framing is lost ends the readable part of the file: the
 *    unreadable tail is moved to a `.damaged-*` sidecar and the file is truncated there, so the
 *    store is consistent again and later writes survive the next open.
 */
class DiskStoreDamageRecoveryTest {

    @TempDir
    lateinit var tempDir: Path

    private lateinit var dataFile: Path
    private val key: ByteArray = Encryption.generateKey()

    @BeforeEach
    fun setup() {
        dataFile = tempDir.resolve("test.data")
    }

    // ------------------------------------------------------------------ failed append rollback

    @ParameterizedTest(name = "encrypted = {0}")
    @ValueSource(booleans = [false, true])
    fun `an append that fails halfway leaves no partial record behind`(encrypted: Boolean) {
        createStore(encrypted).use { store ->
            store.write(1, doc("first"))
            val sizeBefore = store.fileSize()

            store.afterPartialAppendHook = { throw IOException("No space left on device") }
            assertThatThrownBy { store.write(2, doc("second")) }
                .isInstanceOf(IOException::class.java)
            store.afterPartialAppendHook = null

            assertThat(store.fileSize())
                .describedAs("the length prefix written before the failure must be rolled back")
                .isEqualTo(sizeBefore)
            assertThat(store.exists(2)).isFalse()

            // The store keeps working, at a clean boundary.
            store.write(3, doc("third"))
            assertThat(store.read(3)?.get("name")?.asText()).isEqualTo("third")
        }

        createStore(encrypted).use { reopened ->
            assertThat(reopened.isIndexComplete()).isTrue()
            assertThat(reopened.count()).isEqualTo(2)
            assertThat(reopened.read(1)?.get("name")?.asText()).isEqualTo("first")
            assertThat(reopened.read(3)?.get("name")?.asText()).isEqualTo("third")
        }
        assertThat(sidecars()).isEmpty()
    }

    @ParameterizedTest(name = "encrypted = {0}")
    @ValueSource(booleans = [false, true])
    fun `a delete whose tombstone fails halfway is rolled back and the document survives`(encrypted: Boolean) {
        createStore(encrypted).use { store ->
            store.write(1, doc("keep-me"))
            val sizeBefore = store.fileSize()

            store.afterPartialAppendHook = { throw IOException("No space left on device") }
            assertThat(store.delete(1)).isFalse()
            store.afterPartialAppendHook = null

            assertThat(store.fileSize()).isEqualTo(sizeBefore)
            assertThat(store.read(1)?.get("name")?.asText()).isEqualTo("keep-me")
        }

        createStore(encrypted).use { reopened ->
            assertThat(reopened.count()).isEqualTo(1)
            assertThat(reopened.read(1)?.get("name")?.asText()).isEqualTo("keep-me")
        }
    }

    // ------------------------------------------------------------------------- torn tail

    @ParameterizedTest(name = "encrypted = {0}")
    @ValueSource(booleans = [false, true])
    fun `a torn record at the end of the file is quarantined and later writes survive a reopen`(encrypted: Boolean) {
        val intactSize = createStore(encrypted).use { store ->
            store.write(1, doc("first"))
            store.write(2, doc("second"))
            store.write(3, doc("third"))
            store.fileSize()
        }

        // A crash mid-append: a plausible length prefix, then only part of the payload.
        val tornBytes = appendTornRecord(encrypted)

        createStore(encrypted).use { store ->
            assertThat(store.count()).isEqualTo(3)
            assertThat(store.isIndexComplete())
                .describedAs("the damage was removed, so the index covers the whole file")
                .isTrue()
            assertThat(store.fileSize()).isEqualTo(intactSize)

            val sidecar = sidecars().single()
            assertThat(Files.size(sidecar))
                .describedAs("every unreadable byte is preserved, none deleted")
                .isEqualTo(tornBytes.toLong())

            // THE regression: this write used to be lost forever at the next open.
            store.write(4, doc("after-the-damage"))
        }

        createStore(encrypted).use { reopened ->
            assertThat(reopened.count()).isEqualTo(4)
            assertThat(reopened.read(4)?.get("name")?.asText()).isEqualTo("after-the-damage")
            assertThat(reopened.isIndexComplete()).isTrue()
            reopened.compact()   // allowed again: the index is complete
            assertThat(reopened.count()).isEqualTo(4)
        }
        assertThat(sidecars()).describedAs("the sidecar is left for inspection").hasSize(1)
    }

    @ParameterizedTest(name = "encrypted = {0}")
    @ValueSource(booleans = [false, true])
    fun `a zero-filled tail is quarantined in one step instead of parsed as endless empty records`(encrypted: Boolean) {
        val intactSize = createStore(encrypted).use { store ->
            store.write(1, doc("first"))
            store.write(2, doc("second"))
            store.fileSize()
        }

        // What a full disk or a sparse preallocation leaves behind.
        RandomAccessFile(dataFile.toFile(), "rw").use { raf ->
            raf.seek(raf.length())
            raf.write(ByteArray(4096))
        }

        createStore(encrypted).use { store ->
            assertThat(store.count()).isEqualTo(2)
            assertThat(store.isIndexComplete()).isTrue()
            assertThat(store.corruptEntries()).isEqualTo(0)
            assertThat(store.fileSize()).isEqualTo(intactSize)
            assertThat(Files.size(sidecars().single())).isEqualTo(4096L)
        }
    }

    // ------------------------------------------------------------------ framing lost mid-file

    @ParameterizedTest(name = "encrypted = {0}")
    @ValueSource(booleans = [false, true])
    fun `an impossible length in the middle of the file quarantines the tail and keeps the prefix`(encrypted: Boolean) {
        var damagedAt = 0L
        var fullSize = 0L
        createStore(encrypted).use { store ->
            store.write(1, doc("first"))
            store.write(2, doc("second"))
            damagedAt = store.fileSize()
            store.write(3, doc("third"))
            store.write(4, doc("fourth"))
            fullSize = store.fileSize()
        }

        RandomAccessFile(dataFile.toFile(), "rw").use { raf ->
            raf.seek(damagedAt)
            raf.writeInt(999_999_999)
        }

        createStore(encrypted).use { store ->
            assertThat(store.count()).isEqualTo(2)
            assertThat(store.read(1)?.get("name")?.asText()).isEqualTo("first")
            assertThat(store.read(2)?.get("name")?.asText()).isEqualTo("second")
            assertThat(store.isIndexComplete()).isTrue()
            assertThat(store.fileSize()).isEqualTo(damagedAt)
            assertThat(Files.size(sidecars().single())).isEqualTo(fullSize - damagedAt)

            store.write(5, doc("fifth"))
        }

        createStore(encrypted).use { reopened ->
            assertThat(reopened.count()).isEqualTo(3)
            assertThat(reopened.read(5)?.get("name")?.asText()).isEqualTo("fifth")
        }
    }

    // ------------------------------------------------------------- corrupt payload, intact frame

    @Test
    fun `an encrypted record that fails authentication is skipped and everything after it is kept`() {
        var secondAt = 0L
        createStore(encrypted = true).use { store ->
            store.write(1, doc("first"))
            secondAt = store.fileSize()
            store.write(2, doc("second"))
            store.write(3, doc("third"))
            store.write(4, doc("fourth"))
        }

        // Flip one ciphertext byte well inside record 2. The length prefix stays correct, so the
        // framing is intact; GCM authentication is what fails.
        RandomAccessFile(dataFile.toFile(), "rw").use { raf ->
            val target = secondAt + 4 + 20
            raf.seek(target)
            val b = raf.read()
            raf.seek(target)
            raf.write(b xor 0x55)
        }

        val sizeBeforeCompact = createStore(encrypted = true).use { store ->
            assertThat(store.count()).isEqualTo(3)
            assertThat(store.read(2)).isNull()
            assertThat(store.read(3)?.get("name")?.asText()).isEqualTo("third")
            assertThat(store.read(4)?.get("name")?.asText()).isEqualTo("fourth")
            assertThat(store.corruptEntries()).isEqualTo(1)
            assertThat(store.isIndexComplete()).isTrue()
            assertThat(sidecars()).describedAs("nothing was quarantined: framing never broke").isEmpty()

            val before = store.fileSize()
            store.compact()
            assertThat(store.corruptEntries()).isEqualTo(0)
            assertThat(store.fileSize()).isLessThan(before)
            before
        }

        createStore(encrypted = true).use { reopened ->
            assertThat(reopened.count()).isEqualTo(3)
            assertThat(reopened.corruptEntries()).describedAs("compaction dropped the corrupt record").isEqualTo(0)
            assertThat(reopened.fileSize()).isLessThan(sizeBeforeCompact)
        }
    }

    @Test
    fun `an unencrypted record whose ID will not deserialize is skipped and the rest is kept`() {
        var secondAt = 0L
        createStore(encrypted = false).use { store ->
            store.write(1, doc("first"))
            secondAt = store.fileSize()
            store.write(2, doc("second"))
            store.write(3, doc("third"))
        }

        // Replace the single-byte ID "2" with "x": same length, so framing is intact, but the
        // Int deserializer rejects it.
        RandomAccessFile(dataFile.toFile(), "rw").use { raf ->
            raf.seek(secondAt + 4)
            raf.write('x'.code)
        }

        createStore(encrypted = false).use { store ->
            assertThat(store.count()).isEqualTo(2)
            assertThat(store.read(3)?.get("name")?.asText()).isEqualTo("third")
            assertThat(store.corruptEntries()).isEqualTo(1)
            assertThat(store.isIndexComplete()).isTrue()
        }
    }

    // ----------------------------------------------------------------------------- helpers

    private fun createStore(encrypted: Boolean): DiskStore<Int> = DiskStore(
        dataFile = dataFile,
        idSerializer = { it.toString() },
        idDeserializer = { it.toInt() },
        syncOnWrite = false,
        encryptionKey = if (encrypted) key else null
    )

    /** Appends a record whose declared length exceeds the bytes written. Returns bytes appended. */
    private fun appendTornRecord(encrypted: Boolean): Int {
        RandomAccessFile(dataFile.toFile(), "rw").use { raf ->
            raf.seek(raf.length())
            return if (encrypted) {
                raf.writeInt(500)                 // claims 500 bytes of ciphertext...
                raf.write(ByteArray(100) { 7 })   // ...delivers 100
                104
            } else {
                raf.writeInt(1)                   // ID length
                raf.write('9'.code)               // ID
                raf.writeInt(40)                  // claims a 40-byte document...
                raf.write("{\"name\":\"tor".toByteArray())   // ...crash
                4 + 1 + 4 + 12
            }
        }
    }

    private fun sidecars(): List<Path> =
        Files.list(tempDir).use { it.filter { p -> p.name.contains(".damaged-") }.toList() }

    private fun doc(name: String): ObjectNode = DocumentUtils.createDocument().apply {
        put("name", name)
        put("payload", "x".repeat(64))
    }

    private inline fun <T> DiskStore<Int>.use(block: (DiskStore<Int>) -> T): T =
        try { block(this) } finally { close() }
}
