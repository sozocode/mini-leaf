package com.minileaf.core.objectid

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.bson.types.ObjectId
import org.junit.jupiter.api.Test
import java.time.Instant

class ObjectIdUtilsTest {

    @Test
    fun `generate creates valid ObjectId`() {
        val id = ObjectIdUtils.generate()
        assertThat(id).isNotNull
        assertThat(id.toHexString()).hasSize(24)
    }

    @Test
    fun `generate creates unique ObjectIds`() {
        val ids = (1..100).map { ObjectIdUtils.generate() }.toSet()
        assertThat(ids).hasSize(100)
    }

    @Test
    fun `parse valid hex string succeeds`() {
        val hex = "507f1f77bcf86cd799439011"
        val id = ObjectIdUtils.parse(hex)
        assertThat(id.toHexString()).isEqualTo(hex)
    }

    @Test
    fun `parse invalid length throws exception`() {
        assertThatThrownBy { ObjectIdUtils.parse("invalid") }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("24 characters")
    }

    @Test
    fun `parse invalid characters throws exception`() {
        assertThatThrownBy { ObjectIdUtils.parse("507f1f77bcf86cd799439xyz") }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("invalid characters")
    }

    @Test
    fun `tryParse returns null for invalid input`() {
        assertThat(ObjectIdUtils.tryParse("invalid")).isNull()
        assertThat(ObjectIdUtils.tryParse("507f1f77bcf86cd799439xyz")).isNull()
    }

    @Test
    fun `tryParse returns ObjectId for valid input`() {
        val hex = "507f1f77bcf86cd799439011"
        val id = ObjectIdUtils.tryParse(hex)
        assertThat(id).isNotNull
        assertThat(id!!.toHexString()).isEqualTo(hex)
    }

    @Test
    fun `isValid checks hex string format`() {
        assertThat(ObjectIdUtils.isValid("507f1f77bcf86cd799439011")).isTrue
        assertThat(ObjectIdUtils.isValid("invalid")).isFalse
        assertThat(ObjectIdUtils.isValid("507f1f77bcf86cd799439xyz")).isFalse
    }

    @Test
    fun `fromTimestamp creates ObjectId with correct timestamp`() {
        val now = Instant.now()
        val id = ObjectIdUtils.fromTimestamp(now)

        val timestamp = id.timestamp()
        assertThat(timestamp.epochSecond).isEqualTo(now.epochSecond)
    }

    @Test
    fun `forTest creates deterministic ObjectIds`() {
        val id1 = ObjectIdUtils.forTest(1)
        val id2 = ObjectIdUtils.forTest(1)
        val id3 = ObjectIdUtils.forTest(2)

        assertThat(id1.toHexString()).isEqualTo(id2.toHexString())
        assertThat(id1.toHexString()).isNotEqualTo(id3.toHexString())
    }

    @Test
    fun `timestamp extension returns creation time`() {
        val before = Instant.now().minusSeconds(1)
        val id = ObjectIdUtils.generate()
        val after = Instant.now().plusSeconds(1)

        val timestamp = id.timestamp()
        assertThat(timestamp).isBetween(before, after)
    }
}
