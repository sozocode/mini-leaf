package com.minileaf.core

import com.minileaf.core.admin.CollectionHandle
import com.minileaf.core.codec.Codec
import com.minileaf.core.config.MinileafConfig
import com.minileaf.core.repository.IEnhancedSzRepository

/**
 * Main entry point for Minileaf.
 */
interface Minileaf : AutoCloseable {
    /**
     * Gets or creates a repository for the given collection and entity type.
     * Defaults to ObjectId for the ID type.
     */
    fun <T : Any, ID : Comparable<ID>> getRepository(
        collection: String,
        codec: Codec<T>? = null
    ): IEnhancedSzRepository<T, ID>

    /**
     * Gets a handle to a collection for admin operations.
     */
    fun collection(name: String): CollectionHandle

    companion object {
        /**
         * Opens a Minileaf instance with the given configuration.
         */
        fun open(config: MinileafConfig = MinileafConfig()): Minileaf {
            return MinileafImpl(config)
        }
    }
}
