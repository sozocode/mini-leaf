package com.minileaf.jackson

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.module.SimpleModule
import org.bson.BsonBinary
import java.util.Base64

/**
 * Jackson support for [BsonBinary], stored as `{"type": <subtype>, "data": "<base64>"}`.
 *
 * Registered by [JacksonCodec.defaultObjectMapper]. The wire shape is the one the shipped
 * `minimongo-jackson` 1.6.18 wrote, so files produced by that build read back unchanged.
 */
class BsonBinaryModule : SimpleModule() {
    init {
        addSerializer(BsonBinary::class.java, BsonBinarySerializer())
        addDeserializer(BsonBinary::class.java, BsonBinaryDeserializer())
    }
}

class BsonBinarySerializer : JsonSerializer<BsonBinary>() {
    override fun serialize(value: BsonBinary?, gen: JsonGenerator, serializers: SerializerProvider) {
        if (value == null) {
            gen.writeNull()
            return
        }
        gen.writeStartObject()
        gen.writeNumberField("type", value.type.toInt())
        gen.writeStringField("data", Base64.getEncoder().encodeToString(value.data))
        gen.writeEndObject()
    }
}

class BsonBinaryDeserializer : JsonDeserializer<BsonBinary>() {
    override fun deserialize(p: JsonParser, ctxt: DeserializationContext): BsonBinary? {
        if (p.currentToken == JsonToken.VALUE_NULL) return null

        var type: Byte = 0
        var data: ByteArray? = null

        while (p.nextToken() != JsonToken.END_OBJECT) {
            val fieldName = p.currentName
            p.nextToken()
            when (fieldName) {
                "type" -> type = p.intValue.toByte()
                "data" -> data = Base64.getDecoder().decode(p.text)
                else -> p.skipChildren()
            }
        }

        return data?.let { BsonBinary(type, it) }
    }
}
