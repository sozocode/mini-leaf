package com.minileaf.core.storage.memory

import com.minileaf.core.document.DocumentUtils
import com.minileaf.core.exception.DuplicateKeyException
import com.minileaf.core.index.IndexKey
import com.minileaf.core.objectid.ObjectIdUtils
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.bson.types.ObjectId
import org.junit.jupiter.api.Test

class BTreeIndexTest {

    @Test
    fun `single-field index insert and findEquals`() {
        val index = BTreeIndex<ObjectId>(
            indexName = "email_1",
            indexKey = IndexKey("email", 1),
            unique = false
        )

        val id1 = ObjectIdUtils.generate()
        val doc1 = DocumentUtils.createDocument().apply { put("email", "alice@test.com") }
        index.insert(id1, doc1)

        val found = index.findEquals(mapOf("email" to "alice@test.com"))
        assertThat(found).containsExactly(id1)
    }

    @Test
    fun `unique index prevents duplicates`() {
        val index = BTreeIndex<ObjectId>(
            indexName = "email_1",
            indexKey = IndexKey("email", 1),
            unique = true
        )

        val id1 = ObjectIdUtils.generate()
        val id2 = ObjectIdUtils.generate()
        val doc1 = DocumentUtils.createDocument().apply { put("email", "alice@test.com") }
        val doc2 = DocumentUtils.createDocument().apply { put("email", "alice@test.com") }

        index.insert(id1, doc1)

        assertThatThrownBy { index.insert(id2, doc2) }
            .isInstanceOf(DuplicateKeyException::class.java)
            .hasMessageContaining("email_1")
    }

    @Test
    fun `enum-optimized index with hash lookup`() {
        val index = BTreeIndex<ObjectId>(
            indexName = "status_1",
            indexKey = IndexKey("status", 1),
            unique = false,
            enumOptimized = true
        )

        val id1 = ObjectIdUtils.generate()
        val id2 = ObjectIdUtils.generate()
        val doc1 = DocumentUtils.createDocument().apply { put("status", "ACTIVE") }
        val doc2 = DocumentUtils.createDocument().apply { put("status", "ACTIVE") }

        index.insert(id1, doc1)
        index.insert(id2, doc2)

        val found = index.findEquals(mapOf("status" to "ACTIVE"))
        assertThat(found).containsExactlyInAnyOrder(id1, id2)
    }

    @Test
    fun `range query on sorted index`() {
        val index = BTreeIndex<ObjectId>(
            indexName = "age_1",
            indexKey = IndexKey("age", 1),
            unique = false
        )

        val id1 = ObjectIdUtils.generate()
        val id2 = ObjectIdUtils.generate()
        val id3 = ObjectIdUtils.generate()

        index.insert(id1, DocumentUtils.createDocument().apply { put("age", 25) })
        index.insert(id2, DocumentUtils.createDocument().apply { put("age", 30) })
        index.insert(id3, DocumentUtils.createDocument().apply { put("age", 35) })

        val found = index.findRange("age", 28L, 32L)
        assertThat(found).containsExactly(id2)

        val found2 = index.findRange("age", 25L, 35L)
        assertThat(found2).containsExactlyInAnyOrder(id1, id2, id3)
    }

    @Test
    fun `compound index`() {
        val index = BTreeIndex<ObjectId>(
            indexName = "status_1_age_1",
            indexKey = IndexKey(linkedMapOf("status" to 1, "age" to 1)),
            unique = false
        )

        val id1 = ObjectIdUtils.generate()
        val doc1 = DocumentUtils.createDocument().apply {
            put("status", "ACTIVE")
            put("age", 30)
        }
        index.insert(id1, doc1)

        val found = index.findEquals(mapOf("status" to "ACTIVE", "age" to 30L))
        assertThat(found).containsExactly(id1)

        val notFound = index.findEquals(mapOf("status" to "ACTIVE", "age" to 25L))
        assertThat(notFound).isEmpty()
    }

    @Test
    fun `remove deletes from index`() {
        val index = BTreeIndex<ObjectId>(
            indexName = "email_1",
            indexKey = IndexKey("email", 1),
            unique = false
        )

        val id = ObjectIdUtils.generate()
        val doc = DocumentUtils.createDocument().apply { put("email", "alice@test.com") }

        index.insert(id, doc)
        assertThat(index.findEquals(mapOf("email" to "alice@test.com"))).containsExactly(id)

        index.remove(id, doc)
        assertThat(index.findEquals(mapOf("email" to "alice@test.com"))).isEmpty()
    }

    @Test
    fun `update removes old and inserts new`() {
        val index = BTreeIndex<ObjectId>(
            indexName = "email_1",
            indexKey = IndexKey("email", 1),
            unique = false
        )

        val id = ObjectIdUtils.generate()
        val oldDoc = DocumentUtils.createDocument().apply { put("email", "alice@test.com") }
        val newDoc = DocumentUtils.createDocument().apply { put("email", "bob@test.com") }

        index.insert(id, oldDoc)
        index.update(id, oldDoc, newDoc)

        assertThat(index.findEquals(mapOf("email" to "alice@test.com"))).isEmpty()
        assertThat(index.findEquals(mapOf("email" to "bob@test.com"))).containsExactly(id)
    }

    @Test
    fun `clear empties the index`() {
        val index = BTreeIndex<ObjectId>(
            indexName = "email_1",
            indexKey = IndexKey("email", 1),
            unique = false
        )

        repeat(10) { i ->
            val id = ObjectIdUtils.generate()
            val doc = DocumentUtils.createDocument().apply { put("email", "user$i@test.com") }
            index.insert(id, doc)
        }

        index.clear()

        val found = index.findEquals(mapOf("email" to "user0@test.com"))
        assertThat(found).isEmpty()
    }

    @Test
    fun `unique index allows update with same value`() {
        val index = BTreeIndex<ObjectId>(
            indexName = "name_1",
            indexKey = IndexKey("name", 1),
            unique = true
        )

        val id = ObjectIdUtils.generate()
        val doc1 = DocumentUtils.createDocument().apply { put("name", "Default Workspace") }
        val doc2 = DocumentUtils.createDocument().apply { put("name", "Default Workspace") }

        // Insert initial document
        index.insert(id, doc1)

        // Update with same value should not throw exception
        index.update(id, doc1, doc2)

        // Verify the document is still in the index
        val found = index.findEquals(mapOf("name" to "Default Workspace"))
        assertThat(found).containsExactly(id)
    }

    @Test
    fun `unique index allows re-insert of same id with same value`() {
        val index = BTreeIndex<ObjectId>(
            indexName = "name_1",
            indexKey = IndexKey("name", 1),
            unique = true
        )

        val id = ObjectIdUtils.generate()
        val doc = DocumentUtils.createDocument().apply { put("name", "Test") }

        // Insert
        index.insert(id, doc)

        // Re-insert same id with same value should not throw exception
        index.insert(id, doc)

        val found = index.findEquals(mapOf("name" to "Test"))
        assertThat(found).containsExactly(id)
    }
}
