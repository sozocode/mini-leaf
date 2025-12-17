package com.minileaf.core.query

import com.fasterxml.jackson.databind.node.ObjectNode
import com.minileaf.core.document.DocumentUtils

/**
 * Projection for selecting/excluding fields from query results.
 * Supports field inclusion, exclusion, and dot notation.
 */
object Projection {

    /**
     * Applies a projection to a document.
     *
     * @param document The source document
     * @param projection Map of field -> 1 (include) or 0 (exclude)
     * @return Projected document
     *
     * Examples:
     * - { "name": 1, "age": 1 } - Include only name and age
     * - { "password": 0 } - Exclude password field
     * - { "person.name": 1 } - Include nested field
     */
    fun apply(document: ObjectNode, projection: Map<String, Int>): ObjectNode {
        if (projection.isEmpty()) return document

        val isInclusion = projection.values.any { it == 1 }
        val isExclusion = projection.values.any { it == 0 }

        require(!(isInclusion && isExclusion && !projection.containsKey("_id"))) {
            "Cannot mix inclusion and exclusion in projection (except for _id)"
        }

        return if (isInclusion) {
            applyInclusion(document, projection)
        } else {
            applyExclusion(document, projection)
        }
    }

    /**
     * Applies an inclusion projection (select specific fields).
     */
    private fun applyInclusion(document: ObjectNode, projection: Map<String, Int>): ObjectNode {
        val result = DocumentUtils.createDocument()

        // Always include _id unless explicitly excluded
        if (projection["_id"] != 0) {
            document.get("_id")?.let { result.set<ObjectNode>("_id", it) }
        }

        // Include specified fields
        for ((field, value) in projection) {
            if (value == 1 && field != "_id") {
                val node = DocumentUtils.getValueByPath(document, field)
                if (node != null) {
                    setFieldValue(result, field, node)
                }
            }
        }

        return result
    }

    /**
     * Applies an exclusion projection (remove specific fields).
     */
    private fun applyExclusion(document: ObjectNode, projection: Map<String, Int>): ObjectNode {
        val result = document.deepCopy()

        for ((field, value) in projection) {
            if (value == 0) {
                removeField(result, field)
            }
        }

        return result
    }

    /**
     * Sets a field value in the result document, creating nested objects as needed.
     */
    private fun setFieldValue(result: ObjectNode, path: String, value: com.fasterxml.jackson.databind.JsonNode) {
        val parts = path.split('.')
        if (parts.size == 1) {
            result.set<ObjectNode>(path, value)
            return
        }

        var current = result
        for (i in 0 until parts.size - 1) {
            val part = parts[i]
            if (!current.has(part)) {
                current.putObject(part)
            }
            current = current.get(part) as ObjectNode
        }
        current.set<ObjectNode>(parts.last(), value)
    }

    /**
     * Removes a field from the document (supports dot notation).
     */
    private fun removeField(document: ObjectNode, path: String) {
        val parts = path.split('.')
        if (parts.size == 1) {
            document.remove(path)
            return
        }

        var current: ObjectNode? = document
        for (i in 0 until parts.size - 1) {
            val part = parts[i]
            current = current?.get(part) as? ObjectNode
            if (current == null) return
        }
        current?.remove(parts.last())
    }
}
