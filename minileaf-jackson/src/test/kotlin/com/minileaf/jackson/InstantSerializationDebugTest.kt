package com.minileaf.jackson

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.junit.jupiter.api.Test
import java.time.Instant

/**
 * Test to investigate HOW Jackson serializes Instant when using valueToTree()
 * vs when parsing from JSON text.
 */
class InstantSerializationDebugTest {

    data class TestDoc(
        val timestamp: Instant
    )

    @Test
    fun `compare valueToTree vs JSON parsing for Instant`() {
        val mapper = ObjectMapper().apply {
            registerKotlinModule()
            findAndRegisterModules()
        }

        val instant = Instant.parse("2025-10-23T05:37:40.691169Z")
        val doc = TestDoc(timestamp = instant)

        println("=== ORIGINAL INSTANT ===")
        println("Instant: $instant")
        println("Epoch seconds: ${instant.epochSecond}")
        println("Nano: ${instant.nano}")
        println("Epoch millis: ${instant.toEpochMilli()}")

        // METHOD 1: Using valueToTree (what happens during save())
        println("\n=== METHOD 1: valueToTree (SAVE operation) ===")
        val node1 = mapper.valueToTree<ObjectNode>(doc)
        val timestampNode1 = node1.get("timestamp")
        println("Node type: ${timestampNode1.nodeType}")
        println("Node class: ${timestampNode1.javaClass.simpleName}")
        println("isDouble: ${timestampNode1.isDouble}")
        println("isFloatingPointNumber: ${timestampNode1.isFloatingPointNumber}")
        println("isLong: ${timestampNode1.isLong}")
        println("isIntegralNumber: ${timestampNode1.isIntegralNumber}")
        println("asText: ${timestampNode1.asText()}")
        println("asDouble: ${timestampNode1.asDouble()}")
        if (timestampNode1.isFloatingPointNumber) {
            println("Value as double: ${timestampNode1.asDouble()}")
        }
        if (timestampNode1.isLong) {
            println("Value as long: ${timestampNode1.asLong()}")
        }

        // METHOD 2: Parsing from JSON text (what happens during load from disk)
        println("\n=== METHOD 2: Parse from JSON (LOAD from disk) ===")
        val jsonText = mapper.writeValueAsString(doc)
        println("JSON text: $jsonText")
        val node2 = mapper.readTree(jsonText) as ObjectNode
        val timestampNode2 = node2.get("timestamp")
        println("Node type: ${timestampNode2.nodeType}")
        println("Node class: ${timestampNode2.javaClass.simpleName}")
        println("isDouble: ${timestampNode2.isDouble}")
        println("isFloatingPointNumber: ${timestampNode2.isFloatingPointNumber}")
        println("isLong: ${timestampNode2.isLong}")
        println("isIntegralNumber: ${timestampNode2.isIntegralNumber}")
        println("asText: ${timestampNode2.asText()}")
        println("asDouble: ${timestampNode2.asDouble()}")
        if (timestampNode2.isFloatingPointNumber) {
            println("Value as double: ${timestampNode2.asDouble()}")
        }
        if (timestampNode2.isLong) {
            println("Value as long: ${timestampNode2.asLong()}")
        }

        println("\n=== COMPARISON ===")
        println("Same node type: ${timestampNode1.nodeType == timestampNode2.nodeType}")
        println("Same class: ${timestampNode1.javaClass == timestampNode2.javaClass}")
        println("Same value: ${timestampNode1.asDouble() == timestampNode2.asDouble()}")
    }
}
