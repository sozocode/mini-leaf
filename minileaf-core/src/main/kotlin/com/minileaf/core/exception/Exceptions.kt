package com.minileaf.core.exception

/**
 * Base exception for Minileaf.
 */
open class MinileafException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)

/**
 * Thrown when a duplicate key is detected on a unique index.
 */
class DuplicateKeyException(
    val indexName: String,
    val keyValue: Any
) : MinileafException("Duplicate key error on index '$indexName' with value: $keyValue")

/**
 * Thrown when a query is invalid or uses unsupported operators.
 */
class InvalidQueryException(message: String) : MinileafException(message)

/**
 * Thrown when codec conversion fails.
 */
class CodecException(message: String, cause: Throwable? = null) : MinileafException(message, cause)

/**
 * Thrown when storage operations fail.
 */
class StorageException(message: String, cause: Throwable? = null) : MinileafException(message, cause)

/**
 * Thrown when a document exceeds the maximum allowed size.
 */
class DocumentTooLargeException(
    val documentSize: Long,
    val maxSize: Long
) : MinileafException("Document size ($documentSize bytes) exceeds maximum ($maxSize bytes)")
