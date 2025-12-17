package com.minileaf.core.id

import com.fasterxml.jackson.databind.node.ObjectNode
import com.minileaf.core.document.DocumentUtils
import org.bson.types.ObjectId
import java.util.UUID

/**
 * Describes how to handle a specific ID type.
 * Provides serialization, deserialization, generation, extraction, and setting.
 */
interface IdTypeDescriptor<ID : Comparable<ID>> {
    fun serialize(id: ID): String
    fun deserialize(value: String): ID
    fun generate(): ID
    fun extract(document: ObjectNode): ID?
    fun set(document: ObjectNode, id: ID)

    companion object {
        /**
         * Descriptor for MongoDB ObjectId.
         */
        fun objectId(): IdTypeDescriptor<ObjectId> = ObjectIdDescriptor

        /**
         * Descriptor for java.util.UUID.
         */
        fun uuid(): IdTypeDescriptor<UUID> = UUIDDescriptor

        /**
         * Descriptor for String IDs.
         */
        fun string(): IdTypeDescriptor<String> = StringDescriptor

        /**
         * Descriptor for Long IDs.
         */
        fun long(): IdTypeDescriptor<Long> = LongDescriptor
    }
}

/**
 * ObjectId descriptor.
 */
object ObjectIdDescriptor : IdTypeDescriptor<ObjectId> {
    override fun serialize(id: ObjectId): String = id.toHexString()
    override fun deserialize(value: String): ObjectId = ObjectId(value)
    override fun generate(): ObjectId = ObjectId()
    override fun extract(document: ObjectNode): ObjectId? = DocumentUtils.getId(document)
    override fun set(document: ObjectNode, id: ObjectId) = DocumentUtils.setId(document, id)
}

/**
 * UUID descriptor.
 */
object UUIDDescriptor : IdTypeDescriptor<UUID> {
    override fun serialize(id: UUID): String = id.toString()
    override fun deserialize(value: String): UUID = UUID.fromString(value)
    override fun generate(): UUID = UUID.randomUUID()
    override fun extract(document: ObjectNode): UUID? = DocumentUtils.getIdAs<UUID>(document)
    override fun set(document: ObjectNode, id: UUID) = DocumentUtils.setIdGeneric(document, id)
}

/**
 * String descriptor (no auto-generation).
 */
object StringDescriptor : IdTypeDescriptor<String> {
    override fun serialize(id: String): String = id
    override fun deserialize(value: String): String = value
    override fun generate(): String = UUID.randomUUID().toString()
    override fun extract(document: ObjectNode): String? = DocumentUtils.getIdAs<String>(document)
    override fun set(document: ObjectNode, id: String) = DocumentUtils.setIdGeneric(document, id)
}

/**
 * Long descriptor (sequential, not thread-safe for auto-increment).
 */
object LongDescriptor : IdTypeDescriptor<Long> {
    private var counter = 0L
    override fun serialize(id: Long): String = id.toString()
    override fun deserialize(value: String): Long = value.toLong()
    override fun generate(): Long = ++counter
    override fun extract(document: ObjectNode): Long? = DocumentUtils.getIdAs<Long>(document)
    override fun set(document: ObjectNode, id: Long) = DocumentUtils.setIdGeneric(document, id)
}
