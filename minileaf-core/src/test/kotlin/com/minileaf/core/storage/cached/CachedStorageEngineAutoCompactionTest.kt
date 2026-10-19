package com.minileaf.core.storage.cached

import com.fasterxml.jackson.databind.node.ObjectNode
import com.minileaf.core.config.MinileafConfig
import com.minileaf.core.document.DocumentUtils
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

/**
 * Tests for automatic background compaction.
 *
 * Without it, a data file grows forever: updates append rather than overwrite, and nothing ever
 * reclaimed the dead versions. A collection written on a timer reached hundreds of megabytes
 * holding a few kilobytes of live data.
 *
 * Auto-compaction is destructive by nature — it rewrites the file — so these tests care much
 * more about *what survives* than about how much space came back.
 */
class CachedStorageEngineAutoCompactionTest {

    @TempDir
    lateinit var tempDir: Path

    @Test
    @Timeout(30)
    fun `compacts a bloated collection in the background without losing data`() {
        val engine = engine(config(autoCompact = true))

        try {
            // 200 versions of one document: well past the ratio threshold and the size floor.
            repeat(200) { i -> engine.upsert(1, doc("version-$i")) }
            val bloatedSize = dataFileSize()
            assertThat(bloatedSize).isGreaterThan(MIN_BYTES)

            val compactedSize = awaitFileSizeBelow(bloatedSize / 2)

            assertThat(compactedSize)
                .describedAs("file should shrink once dead versions are reclaimed")
                .isLessThan(bloatedSize)

            // The only thing that truly matters: the live document is intact and current.
            assertThat(engine.count()).isEqualTo(1)
            assertThat(engine.findById(1)?.get("name")?.asText()).isEqualTo("version-199")
        } finally {
            engine.close()
        }

        // And it is still intact after a reopen, so the compacted file is well-formed on disk.
        val reopened = engine(config(autoCompact = false))
        try {
            assertThat(reopened.count()).isEqualTo(1)
            assertThat(reopened.findById(1)?.get("name")?.asText()).isEqualTo("version-199")
        } finally {
            reopened.close()
        }
    }

    @Test
    @Timeout(30)
    fun `leaves the file alone when auto-compaction is disabled`() {
        val engine = engine(config(autoCompact = false))
        try {
            repeat(200) { i -> engine.upsert(1, doc("version-$i")) }
            val size = dataFileSize()

            // Give a hypothetical compaction thread several intervals to misbehave.
            Thread.sleep(INTERVAL_MS * 5)

            assertThat(dataFileSize())
                .describedAs("no compaction should run when disabled")
                .isEqualTo(size)
            assertThat(engine.findById(1)?.get("name")?.asText()).isEqualTo("version-199")
        } finally {
            engine.close()
        }
    }

    @Test
    @Timeout(30)
    fun `does not compact a collection below the size floor`() {
        val engine = engine(config(autoCompact = true))
        try {
            // Heavily bloated by ratio (50 versions of one doc) but far under the byte floor.
            repeat(50) { i -> engine.upsert(1, doc("small-$i")) }
            val size = dataFileSize()
            assertThat(size).isLessThan(MIN_BYTES)

            Thread.sleep(INTERVAL_MS * 5)

            assertThat(dataFileSize())
                .describedAs("small files are not worth churning")
                .isEqualTo(size)
        } finally {
            engine.close()
        }
    }

    @Test
    @Timeout(60)
    fun `concurrent writes during auto-compaction are not lost`() {
        // Compaction takes the exclusive lock and swaps the file underneath live traffic. This
        // is the race that would hurt most, so hammer it while compaction keeps re-triggering.
        val engine = engine(config(autoCompact = true))
        val failure = AtomicReference<Throwable?>(null)

        try {
            val writers = (0 until 4).map { w ->
                Thread {
                    try {
                        repeat(250) { i ->
                            val id = w * 1000 + i
                            engine.upsert(id, doc("w$w-$i"))
                            // Rewrite a shared key to keep the bloat ratio climbing.
                            engine.upsert(999_999, doc("churn-$w-$i"))
                        }
                    } catch (t: Throwable) {
                        failure.compareAndSet(null, t)
                    }
                }
            }

            writers.forEach { it.start() }
            writers.forEach { it.join() }

            assertThat(failure.get()).isNull()

            // Every distinct document each writer wrote must still be readable.
            for (w in 0 until 4) {
                for (i in 0 until 250) {
                    val id = w * 1000 + i
                    assertThat(engine.findById(id)?.get("name")?.asText())
                        .describedAs("document $id written by writer $w")
                        .isEqualTo("w$w-$i")
                }
            }
            assertThat(engine.count()).isEqualTo(1001) // 4 x 250 distinct + the churn key
        } finally {
            engine.close()
        }

        // Survives the reopen too — nothing was lost in a swapped-out file.
        val reopened = engine(config(autoCompact = false))
        try {
            assertThat(reopened.count()).isEqualTo(1001)
            assertThat(reopened.findById(3 * 1000 + 249)?.get("name")?.asText()).isEqualTo("w3-249")
        } finally {
            reopened.close()
        }
    }

    @Test
    @Timeout(30)
    fun `close waits for maintenance and leaves no temp file`() {
        val engine = engine(config(autoCompact = true))
        repeat(200) { i -> engine.upsert(1, doc("version-$i")) }

        // Close immediately, quite possibly mid-compaction.
        engine.close()

        assertThat(Files.exists(collectionsDir().resolve("$COLLECTION.data.compact")))
            .describedAs("no compaction leftovers after close")
            .isFalse()

        val reopened = engine(config(autoCompact = false))
        try {
            assertThat(reopened.count()).isEqualTo(1)
            assertThat(reopened.findById(1)?.get("name")?.asText()).isEqualTo("version-199")
        } finally {
            reopened.close()
        }
    }

    // ----------------------------------------------------------------------------- helpers

    private fun config(autoCompact: Boolean) = MinileafConfig(
        dataDir = tempDir,
        cacheSize = 100,
        syncOnWrite = false,
        autoCompactEnabled = autoCompact,
        autoCompactIntervalMs = INTERVAL_MS,
        autoCompactMinBytes = MIN_BYTES,
        autoCompactBloatRatio = 2.0
    )

    private fun engine(config: MinileafConfig) = CachedStorageEngine(
        collectionName = COLLECTION,
        config = config,
        cacheSize = config.cacheSize ?: 100,
        idSerializer = { id: Int -> id.toString() },
        idDeserializer = { s: String -> s.toInt() }
    )

    private fun collectionsDir(): Path = tempDir.resolve("collections")

    private fun dataFileSize(): Long =
        collectionsDir().resolve("$COLLECTION.data").let {
            if (Files.exists(it)) Files.size(it) else 0L
        }

    /** Polls until the data file drops below [target], returning its final size. */
    private fun awaitFileSizeBelow(target: Long): Long {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20)
        var size = dataFileSize()
        while (size >= target && System.nanoTime() < deadline) {
            Thread.sleep(50)
            size = dataFileSize()
        }
        return size
    }

    private fun doc(name: String): ObjectNode = DocumentUtils.createDocument().apply {
        put("name", name)
        put("payload", "x".repeat(256))
    }

    private companion object {
        const val COLLECTION = "autocompact"
        const val INTERVAL_MS = 100L
        const val MIN_BYTES = 16L * 1024
    }
}
