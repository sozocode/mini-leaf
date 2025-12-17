package com.minileaf.core.codec

import com.fasterxml.jackson.databind.node.ObjectNode

/**
 * Codec for converting between entities and JSON documents.
 */
interface Codec<T> {
    /**
     * Converts an entity to an ObjectNode.
     */
    fun toNode(entity: T): ObjectNode

    /**
     * Converts an ObjectNode back to an entity.
     */
    fun fromNode(node: ObjectNode): T

    /**
     * Returns the entity class type.
     */
    fun getEntityClass(): Class<T>
}
