package com.minileaf.core

import com.minileaf.core.admin.CollectionHandle
import com.minileaf.core.codec.Codec
import com.minileaf.core.config.MinileafConfig
import com.minileaf.core.id.IdTypeDescriptor
import com.minileaf.core.repository.IEnhancedSzRepository
import com.minileaf.core.repository.RepositoryImpl
import com.minileaf.core.storage.IndexManager
import com.minileaf.core.storage.StorageEngine
import com.minileaf.core.storage.cached.CachedStorageEngine
import com.minileaf.core.storage.file.FileBackedStorageEngine
import com.minileaf.core.storage.memory.InMemoryStorageEngine
import mu.KotlinLogging
import org.bson.types.ObjectId
import java.util.concurrent.ConcurrentHashMap

/**
 * Main implementation of Minileaf.
 * Defaults to ObjectId for backward compatibility.
 */
class MinileafImpl(
    private val config: MinileafConfig
) : Minileaf {

    private val logger = KotlinLogging.logger {}

    // Collection name -> CollectionData (type-erased storage)
    private val collections = ConcurrentHashMap<String, CollectionData<*>>()

    // Collection name -> CollectionHandle
    private val handles = ConcurrentHashMap<String, CollectionHandle>()

    init {
        logger.info { "Initializing Minileaf with config: $config" }

        // Ensure data directory exists
        if (!config.memoryOnly) {
            config.dataDir.toFile().mkdirs()
        }
    }

    @Suppress("UNCHECKED_CAST")
    override fun <T : Any, ID : Comparable<ID>> getRepository(
        collection: String,
        codec: Codec<T>?
    ): IEnhancedSzRepository<T, ID> {
        // Default to ObjectId for backward compatibility
        return getRepositoryWithIdType(collection, codec, IdTypeDescriptor.objectId() as IdTypeDescriptor<ID>)
    }

    /**
     * Gets a repository with a specific ID type.
     * This is the type-safe entry point for repositories with custom ID types.
     */
    fun <T : Any, ID : Comparable<ID>> getRepositoryWithIdType(
        collection: String,
        codec: Codec<T>?,
        idDescriptor: IdTypeDescriptor<ID>
    ): IEnhancedSzRepository<T, ID> {
        val collectionData = getOrCreateCollection(collection, idDescriptor)

        // Use provided codec or throw
        val actualCodec = codec ?: run {
            logger.warn { "No codec provided for collection '$collection'" }
            throw IllegalArgumentException("Codec must be provided for collection '$collection'")
        }

        @Suppress("UNCHECKED_CAST")
        val storageEngine = collectionData.storageEngine as StorageEngine<ID>
        @Suppress("UNCHECKED_CAST")
        val indexManager = collectionData.indexManager as IndexManager<ID>

        return RepositoryImpl(
            collectionName = collection,
            codec = actualCodec,
            storageEngine = storageEngine,
            indexManager = indexManager,
            config = config,
            idGenerator = idDescriptor::generate,
            idExtractor = idDescriptor::extract,
            idSetter = idDescriptor::set
        )
    }

    override fun collection(name: String): CollectionHandle {
        return handles.computeIfAbsent(name) {
            // Default to ObjectId for collection handles
            val collectionData = getOrCreateCollection(name, IdTypeDescriptor.objectId())
            CollectionHandleImpl(name, collectionData.storageEngine, collectionData.indexManager)
        }
    }

    override fun close() {
        logger.info { "Closing Minileaf..." }

        // Close all collections
        for ((name, collectionData) in collections) {
            try {
                logger.info { "Closing collection '$name'..." }
                collectionData.storageEngine.close()
            } catch (e: Exception) {
                logger.error(e) { "Error closing collection '$name'" }
            }
        }

        collections.clear()
        handles.clear()

        logger.info { "Minileaf closed" }
    }

    /**
     * Gets or creates a collection with the specified ID type.
     * Throws an error if the collection already exists with a different ID type.
     */
    @Suppress("UNCHECKED_CAST")
    private fun <ID : Comparable<ID>> getOrCreateCollection(
        name: String,
        idDescriptor: IdTypeDescriptor<ID>
    ): CollectionData<ID> {
        val existing = collections[name]
        if (existing != null) {
            // Check if ID types match
            val expectedType = idDescriptor.javaClass.simpleName
            if (existing.idDescriptorType != expectedType) {
                throw IllegalArgumentException(
                    "Collection '$name' already exists with ID type ${existing.idDescriptorType}, " +
                    "cannot use with $expectedType. Use a different collection name or restart the application."
                )
            }
            return existing as CollectionData<ID>
        }

        return collections.computeIfAbsent(name) {
            logger.info { "Creating collection '$name' with ID type ${idDescriptor.javaClass.simpleName}..." }

            val storageEngine: StorageEngine<ID> = when {
                config.memoryOnly -> {
                    logger.info { "Using in-memory storage (no persistence)" }
                    InMemoryStorageEngine()
                }
                config.cacheSize != null -> {
                    logger.info { "Using cached storage (cache size: ${config.cacheSize})" }
                    CachedStorageEngine(
                        name,
                        config,
                        config.cacheSize,
                        idDescriptor::serialize,
                        idDescriptor::deserialize
                    )
                }
                else -> {
                    logger.info { "Using file-backed storage (all documents in memory)" }
                    FileBackedStorageEngine(
                        name,
                        config,
                        idDescriptor::serialize,
                        idDescriptor::deserialize
                    )
                }
            }

            val indexManager = IndexManager(name, storageEngine)

            CollectionData(
                storageEngine = storageEngine,
                indexManager = indexManager,
                idDescriptorType = idDescriptor.javaClass.simpleName
            )
        } as CollectionData<ID>
    }

    /**
     * Holds type-erased collection data.
     */
    private data class CollectionData<ID : Comparable<ID>>(
        val storageEngine: StorageEngine<ID>,
        val indexManager: IndexManager<ID>,
        val idDescriptorType: String  // Track the ID descriptor type for validation
    )
}
