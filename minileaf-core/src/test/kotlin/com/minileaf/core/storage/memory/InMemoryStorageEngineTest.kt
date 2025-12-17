package com.minileaf.core.storage.memory

import com.minileaf.core.document.DocumentUtils
import com.minileaf.core.objectid.ObjectIdUtils
import org.assertj.core.api.Assertions.assertThat
import org.bson.types.ObjectId
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class InMemoryStorageEngineTest {

    private lateinit var storage: InMemoryStorageEngine<ObjectId>

    @BeforeEach
    fun setup() {
        storage = InMemoryStorageEngine()
    }

    @Test
    fun `upsert and findById`() {
        val id = ObjectIdUtils.generate()
        val doc = DocumentUtils.createDocument().apply {
            put("name", "Alice")
            put("age", 30)
        }

        storage.upsert(id, doc)

        val found = storage.findById(id)
        assertThat(found).isNotNull
        assertThat(found!!.get("name").asText()).isEqualTo("Alice")
        assertThat(found.get("age").asInt()).isEqualTo(30)
    }

    @Test
    fun `upsert overwrites existing document`() {
        val id = ObjectIdUtils.generate()
        val doc1 = DocumentUtils.createDocument().apply { put("value", 1) }
        val doc2 = DocumentUtils.createDocument().apply { put("value", 2) }

        storage.upsert(id, doc1)
        storage.upsert(id, doc2)

        val found = storage.findById(id)
        assertThat(found!!.get("value").asInt()).isEqualTo(2)
    }

    @Test
    fun `findById returns null for non-existent id`() {
        val id = ObjectIdUtils.generate()
        assertThat(storage.findById(id)).isNull()
    }

    @Test
    fun `delete removes document`() {
        val id = ObjectIdUtils.generate()
        val doc = DocumentUtils.createDocument().apply { put("name", "Alice") }

        storage.upsert(id, doc)
        val deleted = storage.delete(id)

        assertThat(deleted).isNotNull
        assertThat(deleted!!.get("name").asText()).isEqualTo("Alice")
        assertThat(storage.findById(id)).isNull()
    }

    @Test
    fun `delete returns null for non-existent id`() {
        val id = ObjectIdUtils.generate()
        assertThat(storage.delete(id)).isNull()
    }

    @Test
    fun `findAll returns all documents in order`() {
        val id1 = ObjectIdUtils.forTest(1)
        val id2 = ObjectIdUtils.forTest(2)
        val id3 = ObjectIdUtils.forTest(3)

        storage.upsert(id2, DocumentUtils.createDocument().apply { put("name", "B") })
        storage.upsert(id1, DocumentUtils.createDocument().apply { put("name", "A") })
        storage.upsert(id3, DocumentUtils.createDocument().apply { put("name", "C") })

        val all = storage.findAll()
        assertThat(all).hasSize(3)
        assertThat(all[0].first).isEqualTo(id1)
        assertThat(all[1].first).isEqualTo(id2)
        assertThat(all[2].first).isEqualTo(id3)
    }

    @Test
    fun `findAll with pagination`() {
        repeat(10) { i ->
            val id = ObjectIdUtils.forTest(i)
            storage.upsert(id, DocumentUtils.createDocument().apply { put("index", i) })
        }

        val page1 = storage.findAll(skip = 0, limit = 3)
        assertThat(page1).hasSize(3)
        assertThat(page1[0].second.get("index").asInt()).isEqualTo(0)

        val page2 = storage.findAll(skip = 3, limit = 3)
        assertThat(page2).hasSize(3)
        assertThat(page2[0].second.get("index").asInt()).isEqualTo(3)

        val page3 = storage.findAll(skip = 9, limit = 3)
        assertThat(page3).hasSize(1)
    }

    @Test
    fun `count returns correct count`() {
        assertThat(storage.count()).isEqualTo(0)

        storage.upsert(ObjectIdUtils.generate(), DocumentUtils.createDocument())
        assertThat(storage.count()).isEqualTo(1)

        storage.upsert(ObjectIdUtils.generate(), DocumentUtils.createDocument())
        assertThat(storage.count()).isEqualTo(2)
    }

    @Test
    fun `exists checks document existence`() {
        val id = ObjectIdUtils.generate()
        assertThat(storage.exists(id)).isFalse

        storage.upsert(id, DocumentUtils.createDocument())
        assertThat(storage.exists(id)).isTrue

        storage.delete(id)
        assertThat(storage.exists(id)).isFalse
    }

    @Test
    fun `stats returns storage statistics`() {
        val stats = storage.stats()
        assertThat(stats.documentCount).isEqualTo(0)
        assertThat(stats.storageBytes).isEqualTo(0)
        assertThat(stats.walBytes).isEqualTo(0)

        storage.upsert(ObjectIdUtils.generate(), DocumentUtils.createDocument().apply {
            put("data", "some content")
        })

        val stats2 = storage.stats()
        assertThat(stats2.documentCount).isEqualTo(1)
        assertThat(stats2.storageBytes).isGreaterThan(0)
    }
}
