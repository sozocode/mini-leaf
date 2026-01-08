# Minileaf

A lightweight, embeddable Mongo-style document store for Kotlin/JVM apps. Optimized for local use, fast CRUD, simple secondary indexes, and Mongo-like IDs (ObjectId).

---

**From [Sozocode](https://sozocode.com)** - We built Minileaf to power [VisuaLeaf](https://sozocode.com), our MongoDB GUI for developers. Now we're sharing it with the community. If you're working with MongoDB, check out VisuaLeaf for a modern, intuitive database management experience.

---

## Features

- **Embedded**: Runs in-process (no server) for desktop/CLI/services
- **Mongo-like API**: Familiar document-oriented interface with schemaless JSON objects
- **Flexible ID Types**: ObjectId, UUID, String, or Long as document identifiers
- **Flexible Queries**: Dot-path access, array indexes, and comprehensive filter operators
- **Indexes**: Single-field and compound indexes with enum and range optimization
- **Persistence**: File-backed storage with WAL and snapshots for crash-safe recovery
- **Encryption**: Optional AES-256-GCM encryption at rest
- **Type-safe**: Repository pattern with Kotlin generics and data classes

## Quick Start

### Dependencies

**Gradle (Kotlin DSL)**

```kotlin
dependencies {
    implementation("com.sozocode:mini-leaf-core:1.6.13")
    implementation("com.sozocode:mini-leaf-jackson:1.6.13")
    implementation("com.sozocode:mini-leaf-kotlin:1.6.13")
}
```

**Gradle (Groovy)**

```groovy
dependencies {
    implementation 'com.sozocode:mini-leaf-core:1.6.13'
    implementation 'com.sozocode:mini-leaf-jackson:1.6.13'
    implementation 'com.sozocode:mini-leaf-kotlin:1.6.13'
}
```

**Maven**

```xml
<dependencies>
    <dependency>
        <groupId>com.sozocode</groupId>
        <artifactId>mini-leaf-core</artifactId>
        <version>1.6.13</version>
    </dependency>
    <dependency>
        <groupId>com.sozocode</groupId>
        <artifactId>mini-leaf-jackson</artifactId>
        <version>1.6.13</version>
    </dependency>
    <dependency>
        <groupId>com.sozocode</groupId>
        <artifactId>mini-leaf-kotlin</artifactId>
        <version>1.6.13</version>
    </dependency>
</dependencies>
```

### Basic Usage

```kotlin
import com.minileaf.core.Minileaf
import com.minileaf.core.config.MinileafConfig
import com.minileaf.kotlin.repository
import org.bson.types.ObjectId

// Define your entity
data class User(
    var _id: ObjectId? = null,
    val email: String,
    val status: Status,
    val age: Int
)

enum class Status { ACTIVE, INACTIVE }

// Initialize Minileaf
val db = Minileaf.open(MinileafConfig())

// Get a typed repository
val users = db.repository<User, ObjectId>("users")

// Insert
val user = users.save(User(
    email = "alice@example.com",
    status = Status.ACTIVE,
    age = 30
))

// Find by ID
val found = users.findById(user._id!!)

// Query with filters
val activeUsers = users.findAll(
    filter = mapOf("status" to "ACTIVE"),
    skip = 0,
    limit = 10
)

// Close
db.close()
```

### Using Different ID Types

```kotlin
import com.minileaf.kotlin.repositoryWithUUID
import com.minileaf.kotlin.repositoryWithString
import com.minileaf.kotlin.repositoryWithLong
import java.util.UUID

// UUID as ID
data class Session(var _id: UUID? = null, val token: String)
val sessions = db.repositoryWithUUID<Session>("sessions")

// String as ID
data class Config(var _id: String? = null, val value: String)
val configs = db.repositoryWithString<Config>("configs")

// Long as ID
data class Counter(var _id: Long? = null, val count: Int)
val counters = db.repositoryWithLong<Counter>("counters")
```

## Configuration

```kotlin
val config = MinileafConfig(
    dataDir = Paths.get("./minileaf-data"),  // Data directory
    encryptionKey = null,                     // 32 bytes for AES-256-GCM
    autosaveIntervalMs = 5_000,               // Flush buffers every 5s
    snapshotIntervalMs = 60_000,              // Create snapshot every 60s
    walMaxBytesBeforeSnapshot = 64 * 1024 * 1024, // 64 MB
    memoryOnly = false,                       // Set true for in-memory only
    cacheSize = null,                         // LRU cache size (null = all in memory)
    backgroundIndexBuild = true,              // Build indexes in background
    syncOnWrite = true,                       // fsync after each write
    maxDocumentSize = 16 * 1024 * 1024        // 16 MB max document size
)

val db = Minileaf.open(config)
```

## Indexes

```kotlin
import com.minileaf.core.index.IndexKey
import com.minileaf.core.index.IndexOptions

// Single-field unique index
db.collection("users").admin().createIndex(
    IndexKey(linkedMapOf("email" to 1)),
    IndexOptions(unique = true)
)

// Enum-optimized index
db.collection("users").admin().createIndex(
    IndexKey(linkedMapOf("status" to 1)),
    IndexOptions(enumOptimized = true)
)

// Compound index
db.collection("users").admin().createIndex(
    IndexKey(linkedMapOf("status" to 1, "age" to -1))
)

// Drop index
db.collection("users").admin().dropIndex("email_1")

// List indexes
val indexes = db.collection("users").admin().listIndexes()
```

## Query Operators

### Comparison

```kotlin
// Greater than
users.findAll(filter = mapOf("age" to mapOf("\$gt" to 25)))

// Range
users.findAll(filter = mapOf("age" to mapOf("\$gte" to 25, "\$lte" to 35)))

// Not equal
users.findAll(filter = mapOf("status" to mapOf("\$ne" to "INACTIVE")))
```

### Logical

```kotlin
// AND
users.findAll(
    filter = mapOf(
        "\$and" to listOf(
            mapOf("status" to "ACTIVE"),
            mapOf("age" to mapOf("\$gte" to 25))
        )
    )
)

// OR
users.findAll(
    filter = mapOf(
        "\$or" to listOf(
            mapOf("status" to "ACTIVE"),
            mapOf("age" to mapOf("\$lt" to 18))
        )
    )
)
```

### Array & Existence

```kotlin
// In array
users.findAll(filter = mapOf("country" to mapOf("\$in" to listOf("US", "CA"))))

// Not in array
users.findAll(filter = mapOf("country" to mapOf("\$nin" to listOf("XX", "YY"))))

// Field exists
users.findAll(filter = mapOf("middleName" to mapOf("\$exists" to true)))
```

### Pattern Matching

```kotlin
// Regex (case-insensitive)
users.findAll(
    filter = mapOf(
        "email" to mapOf(
            "\$regex" to ".*@example\\.com$",
            "\$options" to "i"
        )
    )
)
```

### Temporal Queries

Query documents by date/time using `Instant` or `LocalDateTime`:

```kotlin
import java.time.Instant
import java.time.temporal.ChronoUnit

data class Metrics(
    var _id: ObjectId? = null,
    val name: String,
    val timestamp: Instant = Instant.now()
)

val metrics = db.repository<Metrics, ObjectId>("metrics")

// Find documents from the last hour
val oneHourAgo = Instant.now().minus(1, ChronoUnit.HOURS)
val recentDocs = metrics.findAll(
    filter = mapOf("timestamp" to mapOf("\$gte" to oneHourAgo)),
    skip = 0,
    limit = 100
)

// Time range query
val startTime = Instant.parse("2024-01-01T00:00:00Z")
val endTime = Instant.parse("2024-01-02T00:00:00Z")
val results = metrics.findAll(
    filter = mapOf(
        "timestamp" to mapOf(
            "\$gte" to startTime,
            "\$lte" to endTime
        )
    ),
    skip = 0,
    limit = 1000
)

// Combine with other filters
val filtered = metrics.findAll(
    filter = mapOf(
        "name" to "cpu",
        "timestamp" to mapOf("\$gte" to oneHourAgo)
    ),
    skip = 0,
    limit = 100
)
```

Supported temporal types: `java.time.Instant`, `java.time.LocalDateTime`

### Dot Notation & Arrays

```kotlin
// Nested field
users.findAll(filter = mapOf("person.address.city" to "San Francisco"))

// Array element
users.findAll(filter = mapOf("phones.0.type" to "mobile"))

// Element match
users.findAll(
    filter = mapOf(
        "phones" to mapOf(
            "\$elemMatch" to mapOf(
                "type" to "mobile",
                "verified" to true
            )
        )
    )
)
```

## Repository API

```kotlin
// Save (upsert)
val user = users.save(User(...))
val batch = users.saveAll(listOf(user1, user2, user3))

// Find
val found = users.findById(id)
val all = users.findAll()
val page = users.findAll(skip = 0, limit = 10)
val filtered = users.findAll(filter = mapOf(...), skip = 0, limit = 10)

// Index-optimized queries
val activeUsers = users.findByEnumField("status", Status.ACTIVE)
val ageRange = users.findByRange("age", 18, 35)

// Delete
val deleted = users.deleteById(id)

// Utilities
val exists = users.exists(id)
val count = users.count()
```

## Advanced Features

### TTL Indexes

Automatically expire documents after a specified duration:

```kotlin
db.collection("sessions").admin().createIndex(
    IndexKey(linkedMapOf("createdAt" to 1)),
    IndexOptions(expireAfterSeconds = 1800) // 30 minutes
)
```

### Partial Indexes

Index only documents matching a filter:

```kotlin
db.collection("users").admin().createIndex(
    IndexKey(linkedMapOf("email" to 1)),
    IndexOptions(
        unique = true,
        partialFilterExpression = mapOf("status" to "ACTIVE")
    )
)
```

### Projections

Select or exclude specific fields from results:

```kotlin
// Inclusion
users.findAll(
    filter = mapOf("status" to "ACTIVE"),
    projection = mapOf("name" to 1, "email" to 1),
    skip = 0,
    limit = 10
)

// Exclusion
users.findAll(
    filter = emptyMap(),
    projection = mapOf("password" to 0, "ssn" to 0),
    skip = 0,
    limit = 10
)
```

### LRU Cache Mode

For large datasets that don't fit in memory:

```kotlin
val config = MinileafConfig(
    cacheSize = 50000  // Keep 50k documents in memory
)
```

See `docs/ADVANCED_FEATURES.md` for detailed documentation.

## Admin & Maintenance

```kotlin
// Collection stats
val stats = db.collection("users").stats()
println("Documents: ${stats.documentCount}")
println("Storage: ${stats.storageBytes} bytes")
println("Indexes: ${stats.indexCount}")

// Force compaction (snapshot + WAL reset)
db.collection("users").compact()
```

## Encryption

```kotlin
import com.minileaf.core.crypto.Encryption

// Generate a key
val key = Encryption.generateKey() // 32 bytes

// Save key securely (e.g., environment variable, key vault)
val keyHex = Encryption.bytesToHex(key)

// Load key and configure Minileaf
val loadedKey = Encryption.hexToBytes(keyHex)
val config = MinileafConfig(encryptionKey = loadedKey)
val db = Minileaf.open(config)
```

## Architecture

### Modules

- **mini-leaf-core**: Storage engine, indexes, query engine, ObjectId utilities
- **mini-leaf-jackson**: Jackson-based codec for entity serialization
- **mini-leaf-kotlin**: Kotlin extensions and helpers

### Storage

- **In-memory mode**: Fastest, no durability
- **File-backed mode**:
  - Write-Ahead Log (WAL) for durability
  - Periodic snapshots for compaction
  - Crash-safe recovery (snapshot + WAL replay)
  - Optional AES-256-GCM encryption
- **Cached mode**: LRU cache for large datasets that don't fit in memory

### Concurrency

- Single-writer / multi-reader per collection
- Document-level atomicity
- MVCC snapshots for readers

## Performance Guidelines

- Primary-key operations: O(log N) via B-tree
- Indexed queries: Index scan + selective fetch
- Create indexes before bulk imports
- Use `backgroundIndexBuild=true` for large datasets
- Use `cacheSize` for datasets larger than available RAM
- Compact periodically for optimal performance

## Examples

See the `examples` module for complete working examples:

```bash
# Run basic example
./gradlew :examples:run

# Run advanced features example (TTL, Partial Indexes, Projections)
./gradlew :examples:runAdvanced

# Run UUID example
./gradlew :examples:runUUID
```

## Testing

In-memory mode is perfect for unit tests:

```kotlin
val db = Minileaf.open(MinileafConfig(memoryOnly = true))
// ... tests ...
db.close()
```

## Limitations

- Max document size: 16 MB (configurable)
- Local DB only, for local application

## License

Copyright © 2025 Sozocode. Licensed under the [Apache License 2.0](LICENSE).
