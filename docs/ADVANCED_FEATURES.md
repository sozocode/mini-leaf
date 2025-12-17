# Minileaf Advanced Features

This guide covers advanced features including TTL indexes, partial indexes, and projections.

## TTL (Time-To-Live) Indexes

TTL indexes automatically delete documents after a specified duration. Perfect for session data, logs, or temporary records.

### Creating a TTL Index

```kotlin
import com.minileaf.core.index.IndexKey
import com.minileaf.core.index.IndexOptions
import java.time.Instant

// Entity with expiration field
data class Session(
    var _id: ObjectId? = null,
    val userId: String,
    val createdAt: Instant,
    val data: String
)

// Create TTL index that expires documents 30 minutes after createdAt
db.collection("sessions").admin().createIndex(
    IndexKey(linkedMapOf("createdAt" to 1)),
    IndexOptions(expireAfterSeconds = 1800) // 30 minutes
)

// Save session - will auto-expire after 30 minutes
sessions.save(Session(
    userId = "user123",
    createdAt = Instant.now(),
    data = "session data"
))
```

### How It Works

1. The TTL index monitors the specified date field
2. Documents expire when: `document[field] + expireAfterSeconds < now()`
3. A background thread periodically checks and removes expired documents
4. Expiration is approximate (may take 60s after actual expiration time)

### Best Practices

- Use `Instant` or epoch timestamps for the TTL field
- Set `expireAfterSeconds` based on your use case:
  - Sessions: 1800-3600s (30min-1hr)
  - Temp data: 300-900s (5-15min)
  - Logs: 604800s (7 days)
- Only one TTL index per collection
- Documents without the TTL field are not affected

## Partial Indexes

Partial indexes only index documents matching a filter, reducing index size and improving performance.

### Creating a Partial Index

```kotlin
// Only index active users
db.collection("users").admin().createIndex(
    IndexKey(linkedMapOf("email" to 1)),
    IndexOptions(
        unique = true,
        partialFilterExpression = mapOf("status" to "ACTIVE")
    )
)

// Only index high-value orders
db.collection("orders").admin().createIndex(
    IndexKey(linkedMapOf("customerId" to 1, "orderDate" to -1)),
    IndexOptions(
        partialFilterExpression = mapOf(
            "total" to mapOf("\$gte" to 1000.0)
        )
    )
)

// Only index documents with specific field present
db.collection("products").admin().createIndex(
    IndexKey(linkedMapOf("category" to 1)),
    IndexOptions(
        partialFilterExpression = mapOf(
            "featured" to mapOf("\$exists" to true)
        )
    )
)
```

### Query Considerations

Partial indexes are used ONLY when:
1. The query filter includes the partial filter expression
2. OR the query uses the index fields

```kotlin
// Uses partial index (includes status filter)
users.findAll(
    filter = mapOf(
        "status" to "ACTIVE",
        "email" to "alice@example.com"
    ),
    skip = 0,
    limit = 10
)

// Does NOT use partial index (missing status filter)
users.findAll(
    filter = mapOf("email" to "alice@example.com"),
    skip = 0,
    limit = 10
)
```

### Benefits

- **Smaller indexes**: Only relevant documents are indexed
- **Faster writes**: Fewer documents to update in index
- **Unique constraints**: Enforce uniqueness only on a subset
- **Cost-effective**: Ideal for large collections with sparse criteria

### Use Cases

1. **Sparse fields**: Index only documents with optional fields
2. **Active records**: Index only current/active entries
3. **Tiered data**: Separate hot/cold data indexing
4. **Soft deletes**: Exclude deleted documents from indexes

## Projections

Projections allow you to select or exclude specific fields from query results.

### Inclusion Projection

Select only specific fields:

```kotlin
// Return only name and email
users.findAll(
    filter = mapOf("status" to "ACTIVE"),
    projection = mapOf("name" to 1, "email" to 1),
    skip = 0,
    limit = 10
)
// Returns: { _id, name, email }

// Exclude _id
users.findAll(
    filter = mapOf("status" to "ACTIVE"),
    projection = mapOf("name" to 1, "email" to 1, "_id" to 0),
    skip = 0,
    limit = 10
)
// Returns: { name, email }
```

### Exclusion Projection

Remove specific fields:

```kotlin
// Exclude sensitive fields
users.findAll(
    filter = emptyMap(),
    projection = mapOf("password" to 0, "ssn" to 0),
    skip = 0,
    limit = 10
)
// Returns: all fields except password and ssn
```

### Nested Field Projection

Use dot notation for nested fields:

```kotlin
// Include only nested fields
users.findAll(
    filter = emptyMap(),
    projection = mapOf(
        "person.name" to 1,
        "person.email" to 1,
        "address.city" to 1
    ),
    skip = 0,
    limit = 10
)
```

### Projection Rules

1. **Cannot mix inclusion and exclusion** (except for `_id`)
   ```kotlin
   // ❌ Invalid: mixing inclusion and exclusion
   mapOf("name" to 1, "password" to 0)

   // ✅ Valid: exclusion only
   mapOf("password" to 0, "ssn" to 0)

   // ✅ Valid: inclusion with _id exclusion
   mapOf("name" to 1, "email" to 1, "_id" to 0)
   ```

2. **_id is always included** unless explicitly excluded
   ```kotlin
   mapOf("name" to 1)         // Returns: { _id, name }
   mapOf("name" to 1, "_id" to 0)  // Returns: { name }
   ```

3. **Nested fields require full path**
   ```kotlin
   mapOf("person" to 1)       // Returns entire person object
   mapOf("person.name" to 1)  // Returns only person.name
   ```

### Performance Benefits

1. **Reduced network transfer**: Smaller result sets
2. **Lower memory usage**: Less data to deserialize
3. **Faster serialization**: Fewer fields to process
4. **Better caching**: Smaller cache entries

### Use Cases

1. **List views**: Return only summary fields (name, id, status)
2. **Security**: Exclude sensitive fields (password, SSN, tokens)
3. **Mobile apps**: Minimize bandwidth usage
4. **Aggregation prep**: Select only fields needed for processing
5. **API responses**: Return only requested fields

## Combining Features

### TTL + Partial Index

```kotlin
// TTL index that only tracks active sessions
db.collection("sessions").admin().createIndex(
    IndexKey(linkedMapOf("expiresAt" to 1)),
    IndexOptions(
        expireAfterSeconds = 0, // Expire at exact time
        partialFilterExpression = mapOf("active" to true)
    )
)
```

### Partial Index + Projection

```kotlin
// Query high-value orders with minimal data transfer
orders.findAll(
    filter = mapOf(
        "total" to mapOf("\$gte" to 1000.0),
        "status" to "PENDING"
    ),
    projection = mapOf("customerId" to 1, "total" to 1, "orderDate" to 1),
    skip = 0,
    limit = 10
)
```

## Configuration

### TTL Background Task

Configure TTL check interval in MinileafConfig:

```kotlin
val config = MinileafConfig(
    ttlCheckIntervalMs = 60_000, // Check every 60 seconds
    // ... other options
)
```

### Index Build Strategy

For large collections with partial indexes:

```kotlin
val config = MinileafConfig(
    backgroundIndexBuild = true, // Build indexes in background
    // ... other options
)
```

## Best Practices

### TTL Indexes
- ✅ Use for temporary data (sessions, caches, temp files)
- ✅ Set reasonable expiration times (not too short)
- ✅ Monitor for unexpected deletions
- ❌ Don't use for critical data without backups
- ❌ Don't rely on exact expiration timing

### Partial Indexes
- ✅ Use for sparse or conditional data
- ✅ Combine with query patterns
- ✅ Document the partial filter in code
- ❌ Don't make filters too complex
- ❌ Don't forget to include filter in queries

### Projections
- ✅ Use for list/summary views
- ✅ Exclude sensitive fields
- ✅ Reduce payload for mobile clients
- ❌ Don't over-optimize (balance readability)
- ❌ Don't project deeply nested structures unnecessarily

## Examples

See `examples/src/main/kotlin/com/minileaf/examples/AdvancedFeaturesExample.kt` for complete working examples.
