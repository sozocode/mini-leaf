package com.minileaf.core.query

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.minileaf.core.document.DocumentUtils
import com.minileaf.core.exception.InvalidQueryException
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset

/**
 * Query engine for evaluating filters against documents.
 * Supports Mongo-like query operators.
 */
object QueryEngine {
    /**
     * Evaluates a filter against a document.
     * @param document The document to test
     * @param filter The query filter (Map-based)
     * @return true if the document matches the filter
     */
    fun matches(document: ObjectNode, filter: Map<String, Any>): Boolean {
        if (filter.isEmpty()) return true

        for ((key, value) in filter) {
            if (key.startsWith("$")) {
                // Logical operators
                if (!evaluateLogicalOperator(document, key, value)) {
                    return false
                }
            } else {
                // Field-level operators
                if (!evaluateFieldCondition(document, key, value)) {
                    return false
                }
            }
        }
        return true
    }

    /**
     * Evaluates a logical operator ($and, $or, $not).
     */
    private fun evaluateLogicalOperator(document: ObjectNode, operator: String, value: Any): Boolean {
        return when (operator) {
            "\$and" -> {
                val conditions = value as? List<*> ?: throw InvalidQueryException("$operator requires a list")
                conditions.all { condition ->
                    @Suppress("UNCHECKED_CAST")
                    matches(document, condition as Map<String, Any>)
                }
            }
            "\$or" -> {
                val conditions = value as? List<*> ?: throw InvalidQueryException("$operator requires a list")
                conditions.any { condition ->
                    @Suppress("UNCHECKED_CAST")
                    matches(document, condition as Map<String, Any>)
                }
            }
            "\$not" -> {
                @Suppress("UNCHECKED_CAST")
                !matches(document, value as Map<String, Any>)
            }
            else -> throw InvalidQueryException("Unsupported logical operator: $operator")
        }
    }

    /**
     * Evaluates a field-level condition.
     */
    private fun evaluateFieldCondition(document: ObjectNode, fieldPath: String, condition: Any): Boolean {
        val actualValue = DocumentUtils.getValueByPath(document, fieldPath)

        // If condition is a map, it might contain operators
        if (condition is Map<*, *>) {
            @Suppress("UNCHECKED_CAST")
            return evaluateFieldOperators(actualValue, condition as Map<String, Any>, fieldPath)
        }

        // Simple equality check
        return if (actualValue == null || actualValue.isNull) {
            condition == null
        } else {
            DocumentUtils.compareValues(actualValue, condition)
        }
    }

    /**
     * Evaluates field operators ($gt, $gte, $lt, $lte, $ne, $in, $nin, $exists, $regex, $elemMatch).
     */
    private fun evaluateFieldOperators(actualValue: JsonNode?, operators: Map<String, Any>, fieldPath: String): Boolean {
        for ((operator, operandValue) in operators) {
            val matches = when (operator) {
                "\$gt" -> {
                    val comparable = DocumentUtils.toComparable(actualValue)
                    comparable != null && compareValues(comparable, operandValue) > 0
                }
                "\$gte" -> {
                    val comparable = DocumentUtils.toComparable(actualValue)
                    comparable != null && compareValues(comparable, operandValue) >= 0
                }
                "\$lt" -> {
                    val comparable = DocumentUtils.toComparable(actualValue)
                    comparable != null && compareValues(comparable, operandValue) < 0
                }
                "\$lte" -> {
                    val comparable = DocumentUtils.toComparable(actualValue)
                    comparable != null && compareValues(comparable, operandValue) <= 0
                }
                "\$ne" -> {
                    actualValue == null || actualValue.isNull || !DocumentUtils.compareValues(actualValue, operandValue)
                }
                "\$in" -> {
                    val values = operandValue as? List<*> ?: throw InvalidQueryException("\$in requires a list")
                    values.any { value -> actualValue != null && DocumentUtils.compareValues(actualValue, value!!) }
                }
                "\$nin" -> {
                    val values = operandValue as? List<*> ?: throw InvalidQueryException("\$nin requires a list")
                    values.none { value -> actualValue != null && DocumentUtils.compareValues(actualValue, value!!) }
                }
                "\$exists" -> {
                    val shouldExist = operandValue as? Boolean ?: throw InvalidQueryException("\$exists requires a boolean")
                    (actualValue != null && !actualValue.isNull) == shouldExist
                }
                "\$regex" -> {
                    actualValue != null && actualValue.isTextual && run {
                        val pattern = operandValue as? String ?: throw InvalidQueryException("\$regex requires a string pattern")
                        val options = operators["\$options"] as? String
                        val regex = if (options?.contains("i") == true) {
                            Regex(pattern, RegexOption.IGNORE_CASE)
                        } else {
                            Regex(pattern)
                        }
                        regex.matches(actualValue.asText())
                    }
                }
                "\$options" -> continue // Handled with $regex
                "\$elemMatch" -> {
                    actualValue != null && actualValue.isArray && run {
                        val elemCondition = operandValue as? Map<*, *> ?: throw InvalidQueryException("\$elemMatch requires a condition")
                        actualValue.any { elem ->
                            elem.isObject && run {
                                @Suppress("UNCHECKED_CAST")
                                matches(elem as ObjectNode, elemCondition as Map<String, Any>)
                            }
                        }
                    }
                }
                else -> throw InvalidQueryException("Unsupported operator: $operator")
            }

            if (!matches) return false
        }
        return true
    }

    /**
     * Compares two values as Comparable.
     * Handles temporal types (Instant, LocalDateTime) by converting to epoch milliseconds.
     */
    @Suppress("UNCHECKED_CAST")
    private fun compareValues(actual: Comparable<*>, expected: Any): Int {
        // Handle temporal types - Jackson can serialize them as:
        // - String (ISO-8601 format like "2024-01-01T12:00:00Z") - default with JavaTimeModule
        // - Number (epoch millis) - when WRITE_DATES_AS_TIMESTAMPS is enabled
        // But filter may contain actual temporal objects (Instant, LocalDateTime)
        if (expected is Instant || expected is LocalDateTime) {
            // Convert filter temporal to epoch millis for comparison
            val expectedMillis = when (expected) {
                is Instant -> expected.toEpochMilli()
                is LocalDateTime -> expected.toInstant(ZoneOffset.UTC).toEpochMilli()
                else -> throw IllegalArgumentException("Unsupported temporal type: ${expected::class}")
            }

            // Convert actual value from document to epoch millis
            val actualMillis = when (actual) {
                // Case 1: Jackson serialized as ISO-8601 string (JavaTimeModule default)
                is String -> {
                    try {
                        Instant.parse(actual).toEpochMilli()
                    } catch (e: Exception) {
                        throw IllegalArgumentException("Cannot parse temporal string '$actual': ${e.message}", e)
                    }
                }
                // Case 2: Jackson serialized as number (WRITE_DATES_AS_TIMESTAMPS enabled or valueToTree)
                // Jackson serializes as SECONDS with decimal precision (e.g., 1704110400.123456789)
                // Values < 10^10 are seconds, values >= 10^10 are milliseconds
                is Long -> {
                    if (actual < 10_000_000_000L) actual * 1000 else actual
                }
                is Int -> {
                    actual.toLong() * 1000  // Always seconds (too small for millis)
                }
                is Double -> {
                    val longValue = actual.toLong()
                    if (longValue < 10_000_000_000L) {
                        // Seconds with decimal precision - convert to millis
                        (actual * 1000).toLong()
                    } else {
                        // Already in milliseconds
                        longValue
                    }
                }
                else -> throw IllegalArgumentException("Cannot compare temporal type with ${actual::class}")
            }

            return actualMillis.compareTo(expectedMillis)
        }

        // Handle other types
        val expectedComparable = when (expected) {
            is Number -> {
                // Normalize to Long or Double for comparison
                when (actual) {
                    is Double, is Float -> expected.toDouble()
                    else -> expected.toLong()
                }
            }
            is Enum<*> -> expected.name
            else -> expected
        } as Comparable<Any>

        return (actual as Comparable<Any>).compareTo(expectedComparable)
    }
}
