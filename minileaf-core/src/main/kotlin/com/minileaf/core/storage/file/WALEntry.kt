package com.minileaf.core.storage.file

import com.fasterxml.jackson.databind.node.ObjectNode
import java.time.Instant

/**
 * Represents a single entry in the Write-Ahead Log.
 * @param ID The type of document identifier
 */
sealed class WALEntry<ID : Any> {
    abstract val timestamp: Instant
    abstract val id: ID

    data class Insert<ID : Any>(
        override val timestamp: Instant,
        override val id: ID,
        val document: ObjectNode
    ) : WALEntry<ID>()

    data class Update<ID : Any>(
        override val timestamp: Instant,
        override val id: ID,
        val document: ObjectNode
    ) : WALEntry<ID>()

    data class Delete<ID : Any>(
        override val timestamp: Instant,
        override val id: ID
    ) : WALEntry<ID>()
}
