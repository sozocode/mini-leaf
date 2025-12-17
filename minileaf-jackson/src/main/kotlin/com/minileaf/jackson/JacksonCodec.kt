package com.minileaf.jackson

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.minileaf.core.codec.Codec
import com.minileaf.core.exception.CodecException
import org.bson.types.ObjectId

/**
 * Default Jackson-based codec for entity conversion.
 */
class JacksonCodec<T : Any>(
    private val entityClass: Class<T>,
    private val objectMapper: ObjectMapper = defaultObjectMapper()
) : Codec<T> {

    override fun toNode(entity: T): ObjectNode {
        try {
            return objectMapper.valueToTree(entity)
        } catch (e: Exception) {
            throw CodecException("Failed to convert entity to ObjectNode: ${e.message}", e)
        }
    }

    override fun fromNode(node: ObjectNode): T {
        try {
            // Convert _id from hex string to ObjectId if needed
            val nodeCopy = node.deepCopy()
            val idNode = nodeCopy.get("_id")
            if (idNode != null && idNode.isTextual) {
                // Check if the entity class has an _id field of type ObjectId
                val idField = try {
                    entityClass.getDeclaredField("_id")
                } catch (e: NoSuchFieldException) {
                    null
                }

                if (idField != null && idField.type == ObjectId::class.java) {
                    // Leave as-is, Jackson will handle conversion with custom serializer
                }
            }

            return objectMapper.treeToValue(nodeCopy, entityClass)
        } catch (e: Exception) {
            throw CodecException("Failed to convert ObjectNode to entity: ${e.message}", e)
        }
    }

    override fun getEntityClass(): Class<T> = entityClass

    companion object {
        /**
         * Creates a default ObjectMapper with Kotlin module.
         */
        fun defaultObjectMapper(): ObjectMapper {
            return ObjectMapper().apply {
                registerKotlinModule()
                findAndRegisterModules()
                // Register custom serializer/deserializer for ObjectId
                registerModule(ObjectIdModule())
            }
        }

        /**
         * Creates a codec for the given entity class.
         */
        inline fun <reified T : Any> create(): JacksonCodec<T> {
            return JacksonCodec(T::class.java)
        }
    }
}
