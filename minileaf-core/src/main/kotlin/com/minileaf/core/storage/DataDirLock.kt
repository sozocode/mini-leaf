package com.minileaf.core.storage

import com.minileaf.core.exception.StorageException
import mu.KotlinLogging
import java.lang.management.ManagementFactory
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileLock
import java.nio.channels.OverlappingFileLockException
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.time.Instant

/**
 * An OS-level exclusive lock on a data directory, held for the lifetime of a store.
 *
 * Two processes appending to the same data files, each with its own in-memory index, do not
 * crash: they silently lose each other's writes and, once either compacts, discard them for
 * good. The usual way into that state is a previous instance that outlived its parent (an
 * orphaned JVM after the desktop shell died) and is still writing. This lock turns that into an
 * immediate, explained failure at open instead.
 *
 * The lock is advisory on POSIX and mandatory on Windows, which is sufficient: the only writers
 * that matter are other instances of this library, and they all take it. The lock file is never
 * deleted — removing a lock file while another process may be racing to open it is the classic
 * way to hand two processes the same lock — so a stale `minileaf.lock` in a data directory is
 * normal and harmless.
 */
class DataDirLock private constructor(
    private val channel: FileChannel,
    private val lock: FileLock,
    val lockFile: Path
) : AutoCloseable {

    override fun close() {
        runCatching { lock.release() }
        runCatching { channel.close() }
    }

    companion object {
        private val logger = KotlinLogging.logger {}

        const val LOCK_FILE_NAME = "minileaf.lock"

        /**
         * Acquires the lock for [dataDir], or throws [StorageException] naming the current holder.
         *
         * Does not block or retry: a holder that is about to exit is indistinguishable from one
         * that will run for hours, and the caller (an application boot) is better placed to decide
         * whether waiting makes sense.
         */
        fun acquire(dataDir: Path): DataDirLock {
            val lockFile = dataDir.resolve(LOCK_FILE_NAME)
            val channel = FileChannel.open(
                lockFile,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
            )

            val lock = try {
                channel.tryLock()
            } catch (e: OverlappingFileLockException) {
                // Another instance in THIS JVM already holds it. Same failure, same message.
                null
            } catch (e: Exception) {
                channel.close()
                throw e
            }

            if (lock == null) {
                val holder = describeHolder(channel)
                channel.close()
                throw StorageException(
                    "Data directory $dataDir is already in use by another process ($holder). " +
                            "Two processes must never share a Minileaf data directory: each would append to " +
                            "the same files with its own index and silently lose the other's writes. " +
                            "If no other instance should be running, find and stop the process holding " +
                            "$lockFile, then retry."
                )
            }

            try {
                channel.truncate(0)
                val stamp = "pid=${ManagementFactory.getRuntimeMXBean().pid}\nsince=${Instant.now()}\n"
                channel.write(ByteBuffer.wrap(stamp.toByteArray(Charsets.UTF_8)), 0)
                channel.force(true)
            } catch (e: Exception) {
                // Diagnostics only; the lock itself is already held.
                logger.debug(e) { "Could not stamp $lockFile with the holder's PID" }
            }

            logger.debug { "Locked data directory $dataDir" }
            return DataDirLock(channel, lock, lockFile)
        }

        /** Best-effort description of who holds the lock, from what they stamped into the file. */
        private fun describeHolder(channel: FileChannel): String {
            return try {
                val size = channel.size().coerceAtMost(4096).toInt()
                if (size == 0) return "holder unknown"
                val buffer = ByteBuffer.allocate(size)
                channel.read(buffer, 0)
                buffer.flip()
                String(buffer.array(), 0, buffer.limit(), Charsets.UTF_8)
                    .trim()
                    .replace('\n', ' ')
                    .ifBlank { "holder unknown" }
            } catch (e: Exception) {
                "holder unknown"
            }
        }
    }
}
