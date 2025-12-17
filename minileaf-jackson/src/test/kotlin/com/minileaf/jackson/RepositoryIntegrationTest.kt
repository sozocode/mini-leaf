package com.minileaf.jackson

import com.minileaf.core.Minileaf
import com.minileaf.core.config.MinileafConfig
import com.minileaf.core.exception.DuplicateKeyException
import com.minileaf.core.index.IndexKey
import com.minileaf.core.index.IndexOptions
import com.minileaf.core.repository.IEnhancedSzRepository
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.bson.types.ObjectId
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class RepositoryIntegrationTest {

    data class TestUser(
        var _id: ObjectId? = null,
        val email: String,
        val status: TestStatus,
        val age: Int,
        val name: String
    )

    enum class TestStatus { ACTIVE, INACTIVE, PENDING }

    private lateinit var mm: Minileaf
    private lateinit var users: IEnhancedSzRepository<TestUser, ObjectId>

    @BeforeEach
    fun setup() {
        mm = Minileaf.open(MinileafConfig(memoryOnly = true))
        users = mm.getRepository("users", JacksonCodec.create())

        // Create indexes
        mm.collection("users").admin().createIndex(
            IndexKey(linkedMapOf("email" to 1)),
            IndexOptions(unique = true)
        )
        mm.collection("users").admin().createIndex(
            IndexKey(linkedMapOf("status" to 1)),
            IndexOptions(enumOptimized = true)
        )
        mm.collection("users").admin().createIndex(
            IndexKey(linkedMapOf("age" to 1))
        )
    }

    @AfterEach
    fun teardown() {
        mm.close()
    }

    @Test
    fun `save generates ObjectId if absent`() {
        val user = TestUser(
            email = "test@example.com",
            status = TestStatus.ACTIVE,
            age = 30,
            name = "Test User"
        )

        val saved = users.save(user)
        assertThat(saved._id).isNotNull
    }

    @Test
    fun `save preserves existing ObjectId`() {
        val originalId = ObjectId()
        val user = TestUser(
            _id = originalId,
            email = "test@example.com",
            status = TestStatus.ACTIVE,
            age = 30,
            name = "Test User"
        )

        val saved = users.save(user)
        assertThat(saved._id).isEqualTo(originalId)
    }

    @Test
    fun `save updates existing document`() {
        val user = users.save(TestUser(
            email = "test@example.com",
            status = TestStatus.ACTIVE,
            age = 30,
            name = "Original Name"
        ))

        val updated = users.save(user.copy(name = "Updated Name"))
        assertThat(updated._id).isEqualTo(user._id)
        assertThat(updated.name).isEqualTo("Updated Name")

        val found = users.findById(user._id!!)
        assertThat(found.get().name).isEqualTo("Updated Name")
    }

    @Test
    fun `unique index prevents duplicate emails`() {
        users.save(TestUser(
            email = "test@example.com",
            status = TestStatus.ACTIVE,
            age = 30,
            name = "User 1"
        ))

        assertThatThrownBy {
            users.save(TestUser(
                email = "test@example.com",
                status = TestStatus.ACTIVE,
                age = 25,
                name = "User 2"
            ))
        }.isInstanceOf(DuplicateKeyException::class.java)
    }

    @Test
    fun `saveAll inserts multiple documents`() {
        val batch = listOf(
            TestUser(email = "user1@test.com", status = TestStatus.ACTIVE, age = 25, name = "User 1"),
            TestUser(email = "user2@test.com", status = TestStatus.ACTIVE, age = 30, name = "User 2"),
            TestUser(email = "user3@test.com", status = TestStatus.INACTIVE, age = 35, name = "User 3")
        )

        val saved = users.saveAll(batch)
        assertThat(saved).hasSize(3)
        assertThat(saved.all { it._id != null }).isTrue
    }

    @Test
    fun `findById returns document`() {
        val user = users.save(TestUser(
            email = "test@example.com",
            status = TestStatus.ACTIVE,
            age = 30,
            name = "Test User"
        ))

        val found = users.findById(user._id!!)
        assertThat(found).isPresent
        assertThat(found.get().email).isEqualTo("test@example.com")
        assertThat(found.get().name).isEqualTo("Test User")
    }

    @Test
    fun `findById returns empty for non-existent id`() {
        val found = users.findById(ObjectId())
        assertThat(found).isEmpty
    }

    @Test
    fun `deleteById removes document`() {
        val user = users.save(TestUser(
            email = "test@example.com",
            status = TestStatus.ACTIVE,
            age = 30,
            name = "Test User"
        ))

        val deleted = users.deleteById(user._id!!)
        assertThat(deleted).isPresent
        assertThat(deleted.get().email).isEqualTo("test@example.com")

        assertThat(users.findById(user._id!!)).isEmpty
    }

    @Test
    fun `findAll returns all documents`() {
        repeat(5) { i ->
            users.save(TestUser(
                email = "user$i@test.com",
                status = TestStatus.ACTIVE,
                age = 20 + i,
                name = "User $i"
            ))
        }

        val all = users.findAll()
        assertThat(all).hasSize(5)
    }

    @Test
    fun `findAll with pagination`() {
        repeat(10) { i ->
            users.save(TestUser(
                email = "user$i@test.com",
                status = TestStatus.ACTIVE,
                age = 20 + i,
                name = "User $i"
            ))
        }

        val page1 = users.findAll(skip = 0, limit = 3)
        assertThat(page1).hasSize(3)

        val page2 = users.findAll(skip = 3, limit = 3)
        assertThat(page2).hasSize(3)

        val page3 = users.findAll(skip = 9, limit = 3)
        assertThat(page3).hasSize(1)
    }

    @Test
    fun `findAll with simple filter`() {
        users.save(TestUser(email = "user1@test.com", status = TestStatus.ACTIVE, age = 25, name = "User 1"))
        users.save(TestUser(email = "user2@test.com", status = TestStatus.INACTIVE, age = 30, name = "User 2"))
        users.save(TestUser(email = "user3@test.com", status = TestStatus.ACTIVE, age = 35, name = "User 3"))

        val active = users.findAll(
            filter = mapOf("status" to "ACTIVE"),
            skip = 0,
            limit = 10
        )
        assertThat(active).hasSize(2)
        assertThat(active.all { it.status == TestStatus.ACTIVE }).isTrue
    }

    @Test
    fun `findAll with range filter`() {
        repeat(10) { i ->
            users.save(TestUser(
                email = "user$i@test.com",
                status = TestStatus.ACTIVE,
                age = 20 + i,
                name = "User $i"
            ))
        }

        val filtered = users.findAll(
            filter = mapOf("age" to mapOf("\$gte" to 25, "\$lte" to 27)),
            skip = 0,
            limit = 10
        )
        assertThat(filtered).hasSize(3)
        assertThat(filtered.all { it.age in 25..27 }).isTrue
    }

    @Test
    fun `findAll with complex filter`() {
        users.save(TestUser(email = "user1@test.com", status = TestStatus.ACTIVE, age = 25, name = "User 1"))
        users.save(TestUser(email = "user2@test.com", status = TestStatus.INACTIVE, age = 30, name = "User 2"))
        users.save(TestUser(email = "user3@test.com", status = TestStatus.ACTIVE, age = 35, name = "User 3"))
        users.save(TestUser(email = "user4@test.com", status = TestStatus.PENDING, age = 28, name = "User 4"))

        val filtered = users.findAll(
            filter = mapOf(
                "\$and" to listOf(
                    mapOf("status" to mapOf("\$ne" to "INACTIVE")),
                    mapOf("age" to mapOf("\$gte" to 25, "\$lte" to 30))
                )
            ),
            skip = 0,
            limit = 10
        )
        assertThat(filtered).hasSize(2) // User 1 and User 4
    }

    @Test
    fun `findByEnumField uses index`() {
        users.save(TestUser(email = "user1@test.com", status = TestStatus.ACTIVE, age = 25, name = "User 1"))
        users.save(TestUser(email = "user2@test.com", status = TestStatus.INACTIVE, age = 30, name = "User 2"))
        users.save(TestUser(email = "user3@test.com", status = TestStatus.ACTIVE, age = 35, name = "User 3"))

        val active = users.findByEnumField("status", TestStatus.ACTIVE)
        assertThat(active).hasSize(2)
        assertThat(active.all { it.status == TestStatus.ACTIVE }).isTrue
    }

    @Test
    fun `findByRange uses index`() {
        repeat(10) { i ->
            users.save(TestUser(
                email = "user$i@test.com",
                status = TestStatus.ACTIVE,
                age = 20 + i,
                name = "User $i"
            ))
        }

        val ranged = users.findByRange("age", 25L, 27L)
        assertThat(ranged).hasSize(3)
        assertThat(ranged.all { it.age in 25..27 }).isTrue
    }

    @Test
    fun `exists returns true for existing document`() {
        val user = users.save(TestUser(
            email = "test@example.com",
            status = TestStatus.ACTIVE,
            age = 30,
            name = "Test User"
        ))

        assertThat(users.exists(user._id!!)).isTrue
    }

    @Test
    fun `exists returns false for non-existent document`() {
        assertThat(users.exists(ObjectId())).isFalse
    }

    @Test
    fun `count returns correct count`() {
        assertThat(users.count()).isEqualTo(0)

        repeat(5) { i ->
            users.save(TestUser(
                email = "user$i@test.com",
                status = TestStatus.ACTIVE,
                age = 20 + i,
                name = "User $i"
            ))
        }

        assertThat(users.count()).isEqualTo(5)
    }
}
