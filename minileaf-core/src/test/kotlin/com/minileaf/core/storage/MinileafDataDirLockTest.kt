package com.minileaf.core.storage

import com.minileaf.core.Minileaf
import com.minileaf.core.config.MinileafConfig
import com.minileaf.core.exception.StorageException
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path

/**
 * One data directory, one process.
 *
 * Two instances on the same directory do not fail; they append to the same files with separate
 * indexes and quietly lose each other's writes. The lock turns that into an immediate error that
 * names the holder.
 */
class MinileafDataDirLockTest {

    @TempDir
    lateinit var tempDir: Path

    @Test
    fun `a second open of the same data directory is refused while the first is live`() {
        val first = Minileaf.open(config())
        try {
            assertThatThrownBy { Minileaf.open(config()) }
                .isInstanceOf(StorageException::class.java)
                .hasMessageContaining("already in use")
                .hasMessageContaining("pid=")
        } finally {
            first.close()
        }
    }

    @Test
    fun `the directory can be reopened once the holder closes`() {
        Minileaf.open(config()).close()

        val second = Minileaf.open(config())
        try {
            second.collection("probe")
        } finally {
            second.close()
        }

        // The lock file is deliberately never deleted; a stale one is harmless.
        assertThat(Files.exists(tempDir.resolve(DataDirLock.LOCK_FILE_NAME))).isTrue()
    }

    @Test
    fun `the lock file records the holder's pid`() {
        val db = Minileaf.open(config())
        try {
            val content = Files.readString(tempDir.resolve(DataDirLock.LOCK_FILE_NAME))
            assertThat(content).startsWith("pid=${ProcessHandle.current().pid()}")
        } finally {
            db.close()
        }
    }

    @Test
    fun `memory-only stores take no lock`() {
        val a = Minileaf.open(MinileafConfig(dataDir = tempDir, memoryOnly = true))
        val b = Minileaf.open(MinileafConfig(dataDir = tempDir, memoryOnly = true))
        a.close()
        b.close()
        assertThat(Files.exists(tempDir.resolve(DataDirLock.LOCK_FILE_NAME))).isFalse()
    }

    @Test
    fun `the lock can be switched off for tooling that knows what it is doing`() {
        val a = Minileaf.open(config(lock = false))
        val b = Minileaf.open(config(lock = false))
        a.close()
        b.close()
    }

    private fun config(lock: Boolean = true) = MinileafConfig(
        dataDir = tempDir,
        cacheSize = 10,
        syncOnWrite = false,
        autoCompactEnabled = false,
        lockDataDir = lock
    )
}
