package com.minileaf.jackson

import com.minileaf.core.Minileaf
import com.minileaf.core.config.MinileafConfig
import com.minileaf.core.index.IndexKey
import com.minileaf.core.index.IndexOptions
import com.minileaf.core.repository.IEnhancedSzRepository
import org.assertj.core.api.Assertions.assertThat
import org.bson.BsonBinary
import org.bson.BsonBinarySubType
import org.bson.types.ObjectId
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

/**
 * The two API additions the shipped 1.6.18 jar had and this repository did not:
 * `deleteAll()` on the repository, and Jackson support for [BsonBinary].
 */
class DeleteAllAndBsonBinaryTest {

    data class Blob(
        var _id: ObjectId? = null,
        val label: String,
        val payload: BsonBinary? = null
    )

    private lateinit var mm: Minileaf
    private lateinit var blobs: IEnhancedSzRepository<Blob, ObjectId>

    @BeforeEach
    fun setup() {
        mm = Minileaf.open(MinileafConfig(memoryOnly = true))
        blobs = mm.getRepository("blobs", JacksonCodec.create())
        mm.collection("blobs").admin().createIndex(
            IndexKey(linkedMapOf("label" to 1)),
            IndexOptions(unique = true)
        )
    }

    @AfterEach
    fun teardown() {
        mm.close()
    }

    @Test
    fun `deleteAll removes every document and reports how many`() {
        blobs.save(Blob(label = "a"))
        blobs.save(Blob(label = "b"))
        blobs.save(Blob(label = "c"))
        assertThat(blobs.count()).isEqualTo(3)

        assertThat(blobs.deleteAll()).isEqualTo(3)

        assertThat(blobs.count()).isEqualTo(0)
        assertThat(blobs.findAll()).isEmpty()
        assertThat(blobs.deleteAll()).describedAs("idempotent on an empty collection").isEqualTo(0)
    }

    @Test
    fun `deleteAll also clears the indexes so the same keys can be reused`() {
        blobs.save(Blob(label = "unique-label"))
        blobs.deleteAll()

        // Would throw DuplicateKeyException if the unique index still held the old entry.
        val again = blobs.save(Blob(label = "unique-label"))
        assertThat(blobs.findById(again._id!!)).isPresent
    }

    @Test
    fun `BsonBinary round-trips through the repository`() {
        val uuid = UUID.randomUUID()
        val binary = BsonBinary(uuid)
        val saved = blobs.save(Blob(label = "uuid", payload = binary))

        val loaded = blobs.findById(saved._id!!).get()
        assertThat(loaded.payload).isNotNull
        assertThat(loaded.payload!!.type).isEqualTo(BsonBinarySubType.UUID_STANDARD.value)
        assertThat(loaded.payload!!.data).isEqualTo(binary.data)
        assertThat(loaded.payload!!.asUuid()).isEqualTo(uuid)
    }

    @Test
    fun `BsonBinary is stored in the shape the shipped 1_6_18 build wrote`() {
        // Files written by minimongo-jackson 1.6.18 must read back unchanged, so the wire shape is
        // part of the contract: {"type": <subtype as int>, "data": "<base64>"}.
        val mapper = JacksonCodec.defaultObjectMapper()
        val json = mapper.writeValueAsString(BsonBinary(byteArrayOf(1, 2, 3)))
        assertThat(json).isEqualTo("""{"type":0,"data":"AQID"}""")

        val back = mapper.readValue(json, BsonBinary::class.java)
        assertThat(back.type).isEqualTo(0.toByte())
        assertThat(back.data).containsExactly(1, 2, 3)

        assertThat(mapper.readValue("null", BsonBinary::class.java)).isNull()
    }
}
