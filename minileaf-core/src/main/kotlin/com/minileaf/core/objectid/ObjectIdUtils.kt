package com.minileaf.core.objectid

import org.bson.types.ObjectId
import java.time.Instant

/**
 * Utilities for working with ObjectIds.
 */
object ObjectIdUtils {
    /**
     * Generates a new ObjectId.
     */
    fun generate(): ObjectId = ObjectId()

    /**
     * Parses an ObjectId from a hex string.
     * @throws IllegalArgumentException if the string is not a valid ObjectId
     */
    fun parse(hex: String): ObjectId {
        require(hex.length == 24) { "ObjectId hex string must be 24 characters, got ${hex.length}" }
        require(hex.all { it in '0'..'9' || it in 'a'..'f' || it in 'A'..'F' }) {
            "ObjectId hex string contains invalid characters"
        }
        return ObjectId(hex)
    }

    /**
     * Tries to parse an ObjectId from a hex string, returning null if invalid.
     */
    fun tryParse(hex: String): ObjectId? = try {
        parse(hex)
    } catch (e: Exception) {
        null
    }

    /**
     * Creates an ObjectId from a timestamp and other components.
     */
    fun fromTimestamp(timestamp: Instant): ObjectId = ObjectId(timestamp.epochSecond.toInt(), 0)

    /**
     * Checks if a string is a valid ObjectId hex string.
     */
    fun isValid(hex: String): Boolean = hex.length == 24 && hex.all {
        it in '0'..'9' || it in 'a'..'f' || it in 'A'..'F'
    }

    /**
     * Creates a deterministic ObjectId for testing (from a counter).
     */
    fun forTest(counter: Int): ObjectId {
        // Create a deterministic ObjectId for testing
        val hex = "%024x".format(counter.toLong())
        return ObjectId(hex)
    }
}

/**
 * Extension: Gets the timestamp from an ObjectId.
 */
fun ObjectId.timestamp(): Instant = Instant.ofEpochSecond(this.timestamp.toLong())

/**
 * Extension: Converts ObjectId to hex string.
 */
fun ObjectId.toHexString(): String = this.toHexString()

/**
 * Extension: Compares two ObjectIds (for sorting).
 */
fun ObjectId.compareTo(other: ObjectId): Int = this.compareTo(other)
