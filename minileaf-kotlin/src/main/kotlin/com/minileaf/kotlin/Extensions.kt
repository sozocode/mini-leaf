package com.minileaf.kotlin

import com.minileaf.core.Minileaf
import com.minileaf.core.MinileafImpl
import com.minileaf.core.codec.Codec
import com.minileaf.core.id.IdTypeDescriptor
import com.minileaf.core.repository.IEnhancedSzRepository
import com.minileaf.jackson.JacksonCodec
import org.bson.types.ObjectId
import java.util.UUID

/**
 * Kotlin extension functions for Minileaf.
 */

/**
 * Gets or creates a repository with automatic codec inference.
 * Defaults to ObjectId for backward compatibility.
 */
inline fun <reified T : Any, ID : Comparable<ID>> Minileaf.repository(
    collection: String
): IEnhancedSzRepository<T, ID> {
    return getRepository(collection, JacksonCodec.create<T>())
}

/**
 * Gets or creates a repository with a custom codec.
 */
fun <T : Any, ID : Comparable<ID>> Minileaf.repository(
    collection: String,
    codec: Codec<T>
): IEnhancedSzRepository<T, ID> {
    return getRepository(collection, codec)
}

/**
 * Gets or creates a repository with UUID as the ID type.
 */
inline fun <reified T : Any> Minileaf.repositoryWithUUID(
    collection: String
): IEnhancedSzRepository<T, UUID> {
    require(this is MinileafImpl) { "repositoryWithUUID requires MinileafImpl" }
    return this.getRepositoryWithIdType(collection, JacksonCodec.create<T>(), IdTypeDescriptor.uuid())
}

/**
 * Gets or creates a repository with String as the ID type.
 */
inline fun <reified T : Any> Minileaf.repositoryWithString(
    collection: String
): IEnhancedSzRepository<T, String> {
    require(this is MinileafImpl) { "repositoryWithString requires MinileafImpl" }
    return this.getRepositoryWithIdType(collection, JacksonCodec.create<T>(), IdTypeDescriptor.string())
}

/**
 * Gets or creates a repository with Long as the ID type.
 */
inline fun <reified T : Any> Minileaf.repositoryWithLong(
    collection: String
): IEnhancedSzRepository<T, Long> {
    require(this is MinileafImpl) { "repositoryWithLong requires MinileafImpl" }
    return this.getRepositoryWithIdType(collection, JacksonCodec.create<T>(), IdTypeDescriptor.long())
}
