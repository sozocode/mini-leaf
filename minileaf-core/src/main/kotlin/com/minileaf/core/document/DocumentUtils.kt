package com.minileaf.core.document

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.bson.types.ObjectId
import java.util.UUID

/**
 * Utilities for working with documents (Jackson ObjectNode).
 */
object DocumentUtils {
    val objectMapper: ObjectMapper = ObjectMapper().apply {
        registerKotlinModule()
        findAndRegisterModules()
    }

    /**
     * Gets the _id or id field from a document as a generic type.
     * Checks _id first (MongoDB standard), then falls back to id for compatibility.
     * Supports ObjectId, UUID, String, Long, Int.
     */
    inline fun <reified ID : Any> getIdAs(document: ObjectNode): ID? {
        // Check _id first (standard), then id (for compatibility)
        val idNode = document.get("_id") ?: document.get("id") ?: return null
        return when (ID::class) {
            ObjectId::class -> when {
                idNode.isTextual -> {
                    try {
                        ObjectId(idNode.asText()) as ID
                    } catch (e: IllegalArgumentException) {
                        null
                    }
                }
                else -> null
            }
            UUID::class -> when {
                idNode.isTextual -> {
                    try {
                        UUID.fromString(idNode.asText()) as ID
                    } catch (e: IllegalArgumentException) {
                        null
                    }
                }
                else -> null
            }
            String::class -> when {
                idNode.isTextual -> idNode.asText() as ID
                else -> null
            }
            Long::class -> when {
                idNode.isNumber -> idNode.asLong() as ID
                else -> null
            }
            Int::class -> when {
                idNode.isNumber -> idNode.asInt() as ID
                else -> null
            }
            else -> null
        }
    }

    /**
     * Gets the _id field from a document as ObjectId (backward compatibility).
     */
    fun getId(document: ObjectNode): ObjectId? = getIdAs<ObjectId>(document)

    /**
     * Sets the ID field in a document with a generic ID type.
     * Writes to whichever field name was originally present (id or _id).
     * Defaults to _id if neither exists.
     */
    fun <ID : Any> setIdGeneric(document: ObjectNode, id: ID) {
        // Determine which field to use: prefer existing field, default to _id
        val fieldName = when {
            document.has("id") && !document.has("_id") -> "id"
            else -> "_id"
        }

        when (id) {
            is ObjectId -> document.put(fieldName, id.toHexString())
            is UUID -> document.put(fieldName, id.toString())
            is String -> document.put(fieldName, id)
            is Long -> document.put(fieldName, id)
            is Int -> document.put(fieldName, id)
            else -> document.put(fieldName, id.toString())
        }
    }

    /**
     * Sets the ID field in a document (backward compatibility).
     * Writes to whichever field name was originally present (id or _id).
     * Defaults to _id if neither exists.
     */
    fun setId(document: ObjectNode, id: ObjectId) {
        // Determine which field to use: prefer existing field, default to _id
        val fieldName = when {
            document.has("id") && !document.has("_id") -> "id"
            else -> "_id"
        }
        document.put(fieldName, id.toHexString())
    }

    /**
     * Gets a value from a document using dot notation.
     * Supports paths like "person.address.city" and array indexes like "phones.0.number".
     */
    fun getValueByPath(document: ObjectNode, path: String): JsonNode? {
        val parts = path.split('.')
        var current: JsonNode? = document

        for (part in parts) {
            if (current == null) return null

            current = when {
                // Array index access
                part.toIntOrNull() != null && current.isArray -> {
                    val index = part.toInt()
                    if (index >= 0 && index < current.size()) current[index] else null
                }
                // Object field access
                current.isObject -> current[part]
                else -> null
            }
        }

        return current
    }

    /**
     * Converts a value to a comparable form for indexing and filtering.
     */
    fun toComparable(node: JsonNode?): Comparable<*>? {
        if (node == null || node.isNull) return null

        return when {
            node.isBoolean -> node.asBoolean()
            node.isInt -> node.asInt().toLong()
            node.isLong -> node.asLong()
            node.isDouble -> node.asDouble()
            node.isFloatingPointNumber -> node.asDouble()  // Handles DecimalNode, FloatNode
            node.isTextual -> {
                val text = node.asText()
                // Try to parse as ObjectId
                if (text.length == 24 && text.all { it in '0'..'9' || it in 'a'..'f' || it in 'A'..'F' }) {
                    try {
                        return ObjectId(text)
                    } catch (e: Exception) {
                        // Not an ObjectId, treat as string
                    }
                }
                text
            }
            else -> null
        }
    }

    /**
     * Creates a new empty ObjectNode.
     */
    fun createDocument(): ObjectNode = objectMapper.createObjectNode()

    /**
     * Converts any object to ObjectNode.
     */
    fun toDocument(value: Any): ObjectNode {
        return objectMapper.valueToTree(value)
    }

    /**
     * Converts ObjectNode to a specific type.
     */
    fun <T> fromDocument(document: ObjectNode, clazz: Class<T>): T {
        return objectMapper.treeToValue(document, clazz)
    }

    /**
     * Checks if a document matches a field value (for simple equality).
     */
    fun matchesField(document: ObjectNode, fieldPath: String, expectedValue: Any): Boolean {
        val actualValue = getValueByPath(document, fieldPath) ?: return false
        return compareValues(actualValue, expectedValue)
    }

    /**
     * Compares two values for equality, handling different types.
     */
    fun compareValues(node: JsonNode, expectedValue: Any): Boolean {
        return when (expectedValue) {
            is String -> node.isTextual && node.asText() == expectedValue
            is Number -> {
                when {
                    node.isInt -> node.asInt().toLong() == expectedValue.toLong()
                    node.isLong -> node.asLong() == expectedValue.toLong()
                    node.isDouble -> node.asDouble() == expectedValue.toDouble()
                    else -> false
                }
            }
            is Boolean -> node.isBoolean && node.asBoolean() == expectedValue
            is ObjectId -> node.isTextual && node.asText() == expectedValue.toHexString()
            is UUID -> node.isTextual && node.asText() == expectedValue.toString()
            is Enum<*> -> node.isTextual && node.asText() == expectedValue.name
            else -> false
        }
    }

    /**
     * Gets all values from an array field (for array contains checks).
     */
    fun getArrayValues(document: ObjectNode, fieldPath: String): List<JsonNode> {
        val node = getValueByPath(document, fieldPath) ?: return emptyList()
        if (!node.isArray) return emptyList()

        val result = mutableListOf<JsonNode>()
        (node as ArrayNode).forEach { result.add(it) }
        return result
    }
}
