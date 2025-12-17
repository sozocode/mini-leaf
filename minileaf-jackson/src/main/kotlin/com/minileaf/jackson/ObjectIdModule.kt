package com.minileaf.jackson

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.module.SimpleModule
import org.bson.types.ObjectId

/**
 * Jackson module for ObjectId serialization/deserialization.
 */
class ObjectIdModule : SimpleModule() {
    init {
        addSerializer(ObjectId::class.java, ObjectIdSerializer())
        addDeserializer(ObjectId::class.java, ObjectIdDeserializer())
    }
}

/**
 * Serializes ObjectId as hex string.
 */
class ObjectIdSerializer : JsonSerializer<ObjectId>() {
    override fun serialize(value: ObjectId?, gen: JsonGenerator, serializers: SerializerProvider) {
        if (value == null) {
            gen.writeNull()
        } else {
            gen.writeString(value.toHexString())
        }
    }
}

/**
 * Deserializes ObjectId from hex string.
 */
class ObjectIdDeserializer : JsonDeserializer<ObjectId>() {
    override fun deserialize(p: JsonParser, ctxt: DeserializationContext): ObjectId? {
        val text = p.text ?: return null
        return if (text.isEmpty()) null else ObjectId(text)
    }
}
