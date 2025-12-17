package com.minileaf.core.index

/**
 * Represents an index key with field names and sort directions.
 * @param fields Map of field name to direction (1 for ascending, -1 for descending)
 */
data class IndexKey(
    val fields: LinkedHashMap<String, Int>
) {
    init {
        require(fields.isNotEmpty()) { "Index must have at least one field" }
        require(fields.values.all { it == 1 || it == -1 }) { "Direction must be 1 (asc) or -1 (desc)" }
    }

    /**
     * Creates a single-field index key.
     */
    constructor(field: String, direction: Int = 1) : this(linkedMapOf(field to direction))

    /**
     * Returns true if this is a single-field index.
     */
    fun isSingleField(): Boolean = fields.size == 1

    /**
     * Returns the first field name.
     */
    fun firstField(): String = fields.keys.first()

    /**
     * Returns the list of field names in order.
     */
    fun fieldNames(): List<String> = fields.keys.toList()
}

/**
 * Options for creating an index.
 */
data class IndexOptions(
    /**
     * If true, the index enforces uniqueness.
     */
    val unique: Boolean = false,

    /**
     * If true, optimize for enum values (hash-based).
     */
    val enumOptimized: Boolean = false,

    /**
     * Optional custom name for the index.
     */
    val name: String? = null,

    /**
     * For TTL indexes: duration in seconds after which documents expire.
     */
    val expireAfterSeconds: Long? = null,

    /**
     * For partial indexes: filter expression determining which documents to index.
     */
    val partialFilterExpression: Map<String, Any>? = null
)

/**
 * Information about an existing index.
 */
data class IndexInfo(
    val name: String,
    val key: IndexKey,
    val unique: Boolean,
    val enumOptimized: Boolean
)
