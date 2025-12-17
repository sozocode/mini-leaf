package com.minileaf.jackson

import com.minileaf.core.Minileaf
import com.minileaf.core.config.MinileafConfig
import org.assertj.core.api.Assertions.assertThat
import org.bson.types.ObjectId
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path

/**
 * Test to verify updateById followed by findById works correctly with CachedStorageEngine
 */
class CachedUpdateFindTest {

    @TempDir
    lateinit var tempDir: Path

    private lateinit var db: Minileaf

    data class User(
        val id: ObjectId = ObjectId(),
        val name: String,
        val age: Int,
        val email: String,
        val visits: Long = 0
    )

    @BeforeEach
    fun setup() {
        val config = MinileafConfig(
            dataDir = tempDir,
            memoryOnly = false,
            cacheSize = 100  // Use cached storage with LRU cache of 100 docs
        )
        db = Minileaf.open(config)
    }

    @AfterEach
    fun cleanup() {
        db.close()
    }

    @Test
    fun `updateById then findById should return updated values with cached storage`() {
        val repository = db.getRepository<User, ObjectId>(
            "users",
            JacksonCodec(User::class.java)
        )

        // 1. Create initial user
        val user = User(
            name = "Alice",
            age = 25,
            email = "alice@example.com",
            visits = 0
        )
        val saved = repository.save(user)
        println("1. Created user: $saved")

        // 2. First findById to load into cache
        val found1 = repository.findById(saved.id).get()
        println("2. First find: $found1")
        assertThat(found1.name).isEqualTo("Alice")
        assertThat(found1.age).isEqualTo(25)
        assertThat(found1.visits).isEqualTo(0)

        // 3. Update the user with $set
        println("3. Updating name to 'Alice Smith' and age to 26...")
        val updated = repository.updateById(
            saved.id,
            mapOf(
                "\$set" to mapOf(
                    "name" to "Alice Smith",
                    "age" to 26
                )
            )
        )
        assertThat(updated).isTrue()
        println("   Update returned: $updated")

        // 4. Find again - should see the updated values
        val found2 = repository.findById(saved.id).get()
        println("4. After update find: $found2")

        // These assertions should pass but currently fail if cache is not checked
        assertThat(found2.name).isEqualTo("Alice Smith")
        assertThat(found2.age).isEqualTo(26)
        assertThat(found2.email).isEqualTo("alice@example.com")
        assertThat(found2.visits).isEqualTo(0)
    }

    @Test
    fun `multiple updates with findById should work correctly`() {
        val repository = db.getRepository<User, ObjectId>(
            "users",
            JacksonCodec(User::class.java)
        )

        // Create user
        val user = User(
            name = "Bob",
            age = 30,
            email = "bob@example.com",
            visits = 10
        )
        val saved = repository.save(user)
        println("Created user: $saved")

        // Load into cache
        repository.findById(saved.id)

        // First update - increment visits
        println("First update: incrementing visits by 5")
        repository.updateById(
            saved.id,
            mapOf("\$inc" to mapOf("visits" to 5L))
        )

        // Check after first update
        val after1 = repository.findById(saved.id).get()
        println("After first update: $after1")
        assertThat(after1.visits).isEqualTo(15)

        // Second update - increment again
        println("Second update: incrementing visits by 3")
        repository.updateById(
            saved.id,
            mapOf("\$inc" to mapOf("visits" to 3L))
        )

        // Check after second update
        val after2 = repository.findById(saved.id).get()
        println("After second update: $after2")
        assertThat(after2.visits).isEqualTo(18)

        // Third update - change name and increment visits
        println("Third update: changing name and incrementing visits")
        repository.updateById(
            saved.id,
            mapOf(
                "\$set" to mapOf("name" to "Bob Smith"),
                "\$inc" to mapOf("visits" to 2L)
            )
        )

        // Final check
        val after3 = repository.findById(saved.id).get()
        println("After third update: $after3")
        assertThat(after3.name).isEqualTo("Bob Smith")
        assertThat(after3.visits).isEqualTo(20)
    }

    @Test
    fun `update without prior findById should work`() {
        val repository = db.getRepository<User, ObjectId>(
            "users",
            JacksonCodec(User::class.java)
        )

        // Create user
        val user = User(
            name = "Charlie",
            age = 35,
            email = "charlie@example.com",
            visits = 100
        )
        val saved = repository.save(user)
        println("Created user: $saved")

        // Update WITHOUT loading into cache first
        println("Updating without prior findById...")
        repository.updateById(
            saved.id,
            mapOf(
                "\$set" to mapOf("name" to "Charlie Brown"),
                "\$inc" to mapOf("visits" to 50L)
            )
        )

        // Now find it
        val found = repository.findById(saved.id).get()
        println("After update: $found")
        assertThat(found.name).isEqualTo("Charlie Brown")
        assertThat(found.visits).isEqualTo(150)
    }
}
