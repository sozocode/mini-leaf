package com.minileaf.examples

import com.minileaf.core.Minileaf
import com.minileaf.core.config.MinileafConfig
import com.minileaf.core.index.IndexKey
import com.minileaf.core.index.IndexOptions
import com.minileaf.kotlin.repository
import org.bson.types.ObjectId
import java.nio.file.Paths

// Domain model
data class User(
    var _id: ObjectId? = null,
    val email: String,
    val status: Status,
    val age: Int,
    val person: Person
)

enum class Status { ACTIVE, INACTIVE }

data class Person(val firstName: String, val lastName: String)

fun main() {
    // Initialize Minileaf
    val db = Minileaf.open(
        MinileafConfig(
            dataDir = Paths.get("./minileaf-data"),
            memoryOnly = false,
            snapshotIntervalMs = 60_000
        )
    )

    // Get a typed repository
    val users = db.repository<User, ObjectId>("users")

    // Create indexes
    db.collection("users").admin().createIndex(
        IndexKey(linkedMapOf("email" to 1)),
        IndexOptions(unique = true)
    )
    db.collection("users").admin().createIndex(
        IndexKey(linkedMapOf("status" to 1)),
        IndexOptions(enumOptimized = true)
    )
    db.collection("users").admin().createIndex(
        IndexKey(linkedMapOf("age" to 1))
    )

    // Insert data
    println("=== Inserting Users ===")
    val user1 = users.save(
        User(
            email = "alice@example.com",
            status = Status.ACTIVE,
            age = 30,
            person = Person("Alice", "Smith")
        )
    )
    println("Created user: ${user1._id}")

    val user2 = users.save(
        User(
            email = "bob@example.com",
            status = Status.ACTIVE,
            age = 25,
            person = Person("Bob", "Jones")
        )
    )
    println("Created user: ${user2._id}")

    val user3 = users.save(
        User(
            email = "charlie@example.com",
            status = Status.INACTIVE,
            age = 35,
            person = Person("Charlie", "Brown")
        )
    )
    println("Created user: ${user3._id}")

    // Find by ID
    println("\n=== Find by ID ===")
    val found = users.findById(user1._id!!)
    println("Found: ${found.get()}")

    // Find all with pagination
    println("\n=== Find All (Paginated) ===")
    val page = users.findAll(skip = 0, limit = 2)
    page.forEach { println("  - ${it.email}") }

    // Query with filters
    println("\n=== Query: Active users ===")
    val activeUsers = users.findAll(
        filter = mapOf("status" to "ACTIVE"),
        skip = 0,
        limit = 10
    )
    activeUsers.forEach { println("  - ${it.email}") }

    // Query with complex filters
    println("\n=== Query: Active users aged 25-35 ===")
    val filteredUsers = users.findAll(
        filter = mapOf(
            "\$and" to listOf(
                mapOf("status" to "ACTIVE"),
                mapOf("age" to mapOf("\$gte" to 25, "\$lte" to 35))
            )
        ),
        skip = 0,
        limit = 10
    )
    filteredUsers.forEach { println("  - ${it.email}, age ${it.age}") }

    // Find by enum field
    println("\n=== Find by Enum Field ===")
    val activeByEnum = users.findByEnumField("status", Status.ACTIVE)
    println("Active users: ${activeByEnum.size}")

    // Find by range
    println("\n=== Find by Range ===")
    val ageRange = users.findByRange("age", 20, 30)
    ageRange.forEach { println("  - ${it.email}, age ${it.age}") }

    // Update
    println("\n=== Update User ===")
    val updated = user1.copy(age = 31)
    users.save(updated)
    println("Updated user ${updated._id}")

    // Delete
    println("\n=== Delete User ===")
    val deleted = users.deleteById(user3._id!!)
    println("Deleted user: ${deleted.get().email}")

    // Count
    println("\n=== Count ===")
    println("Total users: ${users.count()}")

    // Collection stats
    println("\n=== Collection Stats ===")
    val stats = db.collection("users").stats()
    println("Documents: ${stats.documentCount}")
    println("Storage: ${stats.storageBytes} bytes")
    println("Indexes: ${stats.indexCount}")
    stats.indexSizes.forEach { (name, size) ->
        println("  - $name: $size bytes")
    }

    // Close
    db.close()
    println("\n=== Closed Minileaf ===")
}
