package com.minileaf.core.storage.cached

import com.fasterxml.jackson.databind.node.ObjectNode
import com.minileaf.core.document.DocumentUtils
import com.minileaf.core.exception.StorageException
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.RandomAccessFile
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption

/**
 * Crash-safety tests for compaction.
 *
 * Compaction rewrites the whole data file and swaps it into place. Two things can go wrong,
 * and both used to lose data silently:
 *
 * 1. The swap was `delete()` then `renameTo()`. A crash in between left no data file and an
 *    orphaned `.compact` file that nothing on the reopen path looked for — the collection was
 *    simply gone.
 * 2. `buildIndex()` stops at the first damaged record, so the index can legitimately cover
 *    only a prefix of the file. Compacting in that state rewrites the file from the index and
 *    deletes the unindexed tail permanently.
 *
 * These tests pin down the fixes: an atomic swap with recovery on open, and a compaction that
 * refuses to run against an incomplete index.
 */
class DiskStoreCompactionSafetyTest {

    @TempDir
    lateinit var tempDir: Path

    private lateinit var dataFile: Path
    private lateinit var tempCompactFile: Path

    @BeforeEach
    fun setup() {
        dataFile = tempDir.resolve("test.data")
        tempCompactFile = tempDir.resolve("test.data.compact")
    }

    // ---------------------------------------------------------------- interrupted compaction

    @Test
    fun `recovers the compacted file when a crash left no data file`() {
        // Build a store, then stage exactly the wreckage the old delete-then-rename produced:
        // a complete .compact file and no data file.
        createStore().use { store ->
            store.write(1, doc("first"))
            store.write(2, doc("second"))
            store.write(3, doc("third"))
        }
        Files.move(dataFile, tempCompactFile, StandardCopyOption.REPLACE_EXISTING)
        assertThat(Files.exists(dataFile)).isFalse()

        createStore().use { recovered ->
            assertThat(recovered.count()).isEqualTo(3)
            assertThat(recovered.read(1)?.get("name")?.asText()).isEqualTo("first")
            assertThat(recovered.read(2)?.get("name")?.asText()).isEqualTo("second")
            assertThat(recovered.read(3)?.get("name")?.asText()).isEqualTo("third")
        }

        // The temp file is consumed by the recovery, not left to be re-promoted next time.
        assertThat(Files.exists(tempCompactFile)).isFalse()
    }

    @Test
    fun `recovers the compacted file when the data file is present but empty`() {
        // A boot after the old bug would create a fresh zero-byte data file, so the temp file
        // must still win. Anything else silently keeps the empty collection.
        createStore().use { store ->
            store.write(1, doc("survivor"))
        }
        Files.move(dataFile, tempCompactFile, StandardCopyOption.REPLACE_EXISTING)
        Files.createFile(dataFile)

        createStore().use { recovered ->
            assertThat(recovered.count()).isEqualTo(1)
            assertThat(recovered.read(1)?.get("name")?.asText()).isEqualTo("survivor")
        }
    }

    @Test
    fun `discards the temp file when the data file is intact`() {
        // Crash after writing the temp file but before the swap. The data file is authoritative;
        // promoting a temp file of unknown completeness over it would be the unsafe choice.
        createStore().use { store ->
            store.write(1, doc("authoritative"))
        }
        Files.write(tempCompactFile, byteArrayOf(0x01, 0x02, 0x03, 0x04))

        createStore().use { store ->
            assertThat(store.count()).isEqualTo(1)
            assertThat(store.read(1)?.get("name")?.asText()).isEqualTo("authoritative")
        }

        assertThat(Files.exists(tempCompactFile)).isFalse()
    }

    @Test
    fun `compaction leaves no temp file behind`() {
        createStore().use { store ->
            repeat(20) { store.write(1, doc("version-$it")) }
            store.compact()

            assertThat(Files.exists(tempCompactFile)).isFalse()
            assertThat(store.read(1)?.get("name")?.asText()).isEqualTo("version-19")
        }
    }

    @Test
    fun `compaction preserves every live document and drops dead versions`() {
        createStore().use { store ->
            repeat(10) { i -> store.write(i, doc("doc-$i")) }
            repeat(10) { store.write(3, doc("doc-3-updated")) }   // dead versions
            store.delete(7)                                       // tombstone

            val before = store.fileSize()
            store.compact()

            assertThat(store.fileSize()).isLessThan(before)
            assertThat(store.count()).isEqualTo(9)
            assertThat(store.read(3)?.get("name")?.asText()).isEqualTo("doc-3-updated")
            assertThat(store.read(7)).isNull()
            (0..9).filter { it != 7 }.forEach { i ->
                assertThat(store.read(i)).describedAs("document $i").isNotNull()
            }
        }

        // ...and the same holds after a reopen, i.e. the compacted file is well-formed.
        createStore().use { reopened ->
            assertThat(reopened.count()).isEqualTo(9)
            assertThat(reopened.read(3)?.get("name")?.asText()).isEqualTo("doc-3-updated")
            assertThat(reopened.read(7)).isNull()
        }
    }

    // ------------------------------------------------------------------- incomplete index

    @Test
    fun `refuses to compact when a damaged tail could not be quarantined`() {
        // Normally a damaged tail is moved to a sidecar and the file truncated, which leaves the
        // index complete. The incomplete-index state is the fallback for when that move fails —
        // here, because the directory will not accept a new file. Compaction must then refuse:
        // it would rewrite the file from an index that is missing the tail, deleting it for good.
        val corruptedAt = createStore().use { store ->
            store.write(1, doc("first"))
            store.write(2, doc("second"))
            store.write(3, doc("third"))
            val offset = store.fileSize()
            store.write(4, doc("unreachable"))
            offset
        }

        // Corrupt the 4th record's header so the scan loses framing there.
        RandomAccessFile(dataFile.toFile(), "rw").use { raf ->
            raf.seek(corruptedAt)
            raf.writeInt(999_999)
        }

        withReadOnlyDirectory {
            createStore().use { store ->
                assertThat(store.isIndexComplete()).isFalse()
                assertThat(store.incompleteTailOffset()).isEqualTo(corruptedAt)

                // Reads still work — degradation stays graceful.
                assertThat(store.read(1)?.get("name")?.asText()).isEqualTo("first")
                assertThat(store.count()).isEqualTo(3)

                // But compaction must refuse: it would delete the tail for good.
                assertThatThrownBy { store.compact() }
                    .isInstanceOf(StorageException::class.java)
                    .hasMessageContaining("index is incomplete")

                // Refusing means refusing to touch anything.
                assertThat(store.fileSize()).isGreaterThan(corruptedAt)
                assertThat(Files.exists(tempCompactFile)).isFalse()
            }
        }
    }

    @Test
    fun `reports a complete index for an undamaged file`() {
        createStore().use { store ->
            repeat(5) { i -> store.write(i, doc("doc-$i")) }
            assertThat(store.isIndexComplete()).isTrue()
            assertThat(store.incompleteTailOffset()).isEqualTo(-1)
        }

        createStore().use { reopened ->
            assertThat(reopened.isIndexComplete()).isTrue()
            reopened.compact()   // must not throw
            assertThat(reopened.count()).isEqualTo(5)
        }
    }

    @Test
    fun `once the tail can be quarantined the index is complete again and compaction resumes`() {
        createStore().use { store -> store.write(1, doc("only")) }

        // Damage the tail; first reopen cannot quarantine (read-only dir) and runs incomplete.
        val size = Files.size(dataFile)
        RandomAccessFile(dataFile.toFile(), "rw").use { raf ->
            raf.seek(size)
            raf.writeInt(999_999)
        }
        withReadOnlyDirectory {
            createStore().use { store -> assertThat(store.isIndexComplete()).isFalse() }
        }

        // Second reopen can write the sidecar: the junk is quarantined, the flag clears itself.
        createStore().use { store ->
            assertThat(store.isIndexComplete()).isTrue()
            assertThat(store.fileSize()).isEqualTo(size)
            store.compact()
            assertThat(store.read(1)?.get("name")?.asText()).isEqualTo("only")
        }
    }

    /** Runs [block] with the temp directory refusing new files, then restores it. */
    private fun withReadOnlyDirectory(block: () -> Unit) {
        val original = Files.getPosixFilePermissions(tempDir)
        Files.setPosixFilePermissions(tempDir, setOf(
            java.nio.file.attribute.PosixFilePermission.OWNER_READ,
            java.nio.file.attribute.PosixFilePermission.OWNER_EXECUTE
        ))
        try {
            block()
        } finally {
            Files.setPosixFilePermissions(tempDir, original)
        }
    }

    // ------------------------------------------------------------------------ bloat ratio

    @Test
    fun `bloat ratio is one for distinct documents and rises with rewrites`() {
        createStore().use { store ->
            assertThat(store.bloatRatio()).isEqualTo(0.0)

            repeat(10) { i -> store.write(i, doc("doc-$i")) }
            assertThat(store.bloatRatio()).isEqualTo(1.0)

            repeat(90) { store.write(0, doc("rewritten")) }
            // 100 physical entries, 10 live documents.
            assertThat(store.bloatRatio()).isEqualTo(10.0)
        }
    }

    @Test
    fun `bloat ratio survives a reopen and resets after compaction`() {
        createStore().use { store ->
            repeat(50) { store.write(1, doc("v$it")) }
        }

        createStore().use { reopened ->
            // Rebuilt from the file, not carried over in memory.
            assertThat(reopened.bloatRatio()).isEqualTo(50.0)

            reopened.compact()
            assertThat(reopened.bloatRatio()).isEqualTo(1.0)
            assertThat(reopened.read(1)?.get("name")?.asText()).isEqualTo("v49")
        }
    }

    @Test
    fun `bloat ratio is maximal when every document is deleted`() {
        createStore().use { store ->
            repeat(5) { i -> store.write(i, doc("doc-$i")) }
            repeat(5) { i -> store.delete(i) }

            // Nothing live, but the file is all tombstones and dead versions.
            assertThat(store.count()).isEqualTo(0)
            assertThat(store.bloatRatio()).isEqualTo(Double.MAX_VALUE)

            store.compact()
            assertThat(store.fileSize()).isEqualTo(0)
            assertThat(store.count()).isEqualTo(0)
        }
    }

    // ----------------------------------------------------------------------------- helpers

    private fun createStore(): DiskStore<Int> = DiskStore(
        dataFile = dataFile,
        idSerializer = { it.toString() },
        idDeserializer = { it.toInt() },
        syncOnWrite = false   // durability is not what these tests exercise; keeps them quick
    )

    private fun doc(name: String): ObjectNode = DocumentUtils.createDocument().apply {
        put("name", name)
        put("payload", "x".repeat(64))
    }

    private inline fun <T> DiskStore<Int>.use(block: (DiskStore<Int>) -> T): T =
        try { block(this) } finally { close() }
}
