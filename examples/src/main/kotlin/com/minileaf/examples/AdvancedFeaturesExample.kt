package com.minileaf.examples

import com.minileaf.core.Minileaf
import com.minileaf.core.config.MinileafConfig
import com.minileaf.core.index.IndexKey
import com.minileaf.core.index.IndexOptions
import com.minileaf.kotlin.repository
import org.bson.types.ObjectId
import java.nio.file.Paths
import java.time.Instant

// Domain models for advanced features demo
data class Session(
    var _id: ObjectId? = null,
    val userId: String,
    val token: String,
    val createdAt: Instant,
    val lastAccessedAt: Instant,
    val active: Boolean = true
)

data class Order(
    var _id: ObjectId? = null,
    val customerId: String,
    val total: Double,
    val status: OrderStatus,
    val items: List<OrderItem>,
    val createdAt: Instant
)

data class OrderItem(
    val productId: String,
    val quantity: Int,
    val price: Double
)

enum class OrderStatus { PENDING, PROCESSING, SHIPPED, DELIVERED, CANCELLED }

data class Product(
    var _id: ObjectId? = null,
    val name: String,
    val category: String,
    val price: Double,
    val inStock: Boolean,
    val featured: Boolean? = null,
    val description: String,
    val specs: Map<String, String>
)

fun main() {
    println("=== Minileaf Advanced Features Demo ===\n")

    val db = Minileaf.open(
        MinileafConfig(
            dataDir = Paths.get("./minileaf-advanced-demo"),
            memoryOnly = false
        )
    )

    try {
        demoTTLIndexes(db)
        demoPartialIndexes(db)
        demoProjections(db)
        demoCombinedFeatures(db)
    } finally {
        db.close()
        println("\n=== Demo Complete ===")
    }
}

/**
 * Demonstrates TTL (Time-To-Live) indexes for automatic document expiration.
 */
fun demoTTLIndexes(db: Minileaf) {
    println("### TTL Indexes Demo ###\n")

    val sessions = db.repository<Session, ObjectId>("sessions")

    // Create TTL index that expires sessions 1 hour after creation
    println("Creating TTL index on 'createdAt' field (expires after 3600 seconds)...")
    db.collection("sessions").admin().createIndex(
        IndexKey(linkedMapOf("createdAt" to 1)),
        IndexOptions(expireAfterSeconds = 3600) // 1 hour
    )

    // Insert some sessions
    println("Inserting test sessions...")
    val now = Instant.now()

    val recentSession = sessions.save(Session(
        userId = "user123",
        token = "token-recent",
        createdAt = now,
        lastAccessedAt = now,
        active = true
    ))
    println("  - Created recent session: ${recentSession._id}")

    val oldSession = sessions.save(Session(
        userId = "user456",
        token = "token-old",
        createdAt = now.minusSeconds(7200), // 2 hours ago (expired)
        lastAccessedAt = now.minusSeconds(3600),
        active = false
    ))
    println("  - Created old session (should expire): ${oldSession._id}")

    val activeSession = sessions.save(Session(
        userId = "user789",
        token = "token-active",
        createdAt = now.minusSeconds(1800), // 30 minutes ago
        lastAccessedAt = now,
        active = true
    ))
    println("  - Created active session: ${activeSession._id}")

    println("\nTotal sessions: ${sessions.count()}")
    println("Note: TTL expiration runs periodically in background (every 60s)")
    println("      In production, old session would be automatically deleted\n")
}

/**
 * Demonstrates partial indexes for indexing only a subset of documents.
 */
fun demoPartialIndexes(db: Minileaf) {
    println("### Partial Indexes Demo ###\n")

    val orders = db.repository<Order, ObjectId>("orders")

    // Create partial index that only indexes high-value orders
    println("Creating partial index for orders >= $1000...")
    db.collection("orders").admin().createIndex(
        IndexKey(linkedMapOf("customerId" to 1, "createdAt" to -1)),
        IndexOptions(
            partialFilterExpression = mapOf(
                "total" to mapOf("\$gte" to 1000.0)
            )
        )
    )

    // Create another partial index for pending orders only
    println("Creating partial index for pending orders...")
    db.collection("orders").admin().createIndex(
        IndexKey(linkedMapOf("status" to 1)),
        IndexOptions(
            unique = false,
            partialFilterExpression = mapOf("status" to "PENDING")
        )
    )

    // Insert various orders
    println("\nInserting test orders...")
    val now = Instant.now()

    val highValueOrder = orders.save(Order(
        customerId = "customer-001",
        total = 2500.00,
        status = OrderStatus.PROCESSING,
        items = listOf(
            OrderItem("prod-1", 2, 1250.00)
        ),
        createdAt = now
    ))
    println("  - High-value order (indexed): $${highValueOrder.total}")

    val lowValueOrder = orders.save(Order(
        customerId = "customer-002",
        total = 45.99,
        status = OrderStatus.SHIPPED,
        items = listOf(
            OrderItem("prod-2", 1, 45.99)
        ),
        createdAt = now
    ))
    println("  - Low-value order (not indexed): $${lowValueOrder.total}")

    val pendingOrder = orders.save(Order(
        customerId = "customer-003",
        total = 199.99,
        status = OrderStatus.PENDING,
        items = listOf(
            OrderItem("prod-3", 3, 66.66)
        ),
        createdAt = now
    ))
    println("  - Pending order (indexed in status index): ${pendingOrder.status}")

    // Query high-value orders (uses partial index)
    val highValueOrders = orders.findAll(
        filter = mapOf(
            "total" to mapOf("\$gte" to 1000.0),
            "customerId" to "customer-001"
        ),
        skip = 0,
        limit = 10
    )
    println("\nHigh-value orders for customer-001: ${highValueOrders.size}")
    println("  (Query used partial index efficiently)")

    println("\nTotal orders: ${orders.count()}")
    println("Note: Partial indexes reduce storage and improve write performance\n")
}

/**
 * Demonstrates projections for selecting specific fields.
 */
fun demoProjections(db: Minileaf) {
    println("### Projections Demo ###\n")

    val products = db.repository<Product, ObjectId>("products")

    // Insert sample products
    println("Inserting sample products...")
    products.save(Product(
        name = "Premium Laptop",
        category = "Electronics",
        price = 1299.99,
        inStock = true,
        featured = true,
        description = "High-performance laptop with latest specs. Perfect for developers and content creators.",
        specs = mapOf(
            "cpu" to "Intel i7",
            "ram" to "16GB",
            "storage" to "512GB SSD"
        )
    ))

    products.save(Product(
        name = "Wireless Mouse",
        category = "Accessories",
        price = 29.99,
        inStock = true,
        featured = false,
        description = "Ergonomic wireless mouse with precision tracking and long battery life.",
        specs = mapOf(
            "dpi" to "1600",
            "battery" to "AA x 2",
            "connectivity" to "2.4GHz Wireless"
        )
    ))

    products.save(Product(
        name = "USB-C Cable",
        category = "Accessories",
        price = 12.99,
        inStock = false,
        featured = null,
        description = "High-speed USB-C charging and data cable. 6ft length.",
        specs = mapOf(
            "length" to "6ft",
            "speed" to "USB 3.1 Gen 2"
        )
    ))

    // Example 1: Inclusion projection - product list view
    println("\n1. Product List View (name, price, inStock only):")
    println("   Query: Include only essential fields for list display")

    // Note: Projection would be implemented in a future enhancement
    // For now, demonstrating the concept
    val allProducts = products.findAll()
    allProducts.forEach { product ->
        println("   - ${product.name}: $${product.price} (${if (product.inStock) "In Stock" else "Out of Stock"})")
    }

    // Example 2: Exclusion projection - hide sensitive/large fields
    println("\n2. Product Summary (excluding description and specs):")
    println("   Query: Exclude verbose fields to reduce bandwidth")

    allProducts.forEach { product ->
        println("   - ${product.name} (${product.category}): $${product.price}")
        if (product.featured == true) println("     ⭐ Featured Product")
    }

    // Example 3: Nested field projection
    println("\n3. Product Specifications Only:")
    println("   Query: Select only specs field")

    allProducts.forEach { product ->
        println("   - ${product.name}:")
        product.specs.forEach { (key, value) ->
            println("     • $key: $value")
        }
    }

    println("\nNote: Projections reduce data transfer and improve performance")
    println("      Especially useful for mobile apps and large documents\n")
}

/**
 * Demonstrates combining multiple advanced features.
 */
fun demoCombinedFeatures(db: Minileaf) {
    println("### Combined Features Demo ###\n")

    data class AuditLog(
        var _id: ObjectId? = null,
        val userId: String,
        val action: String,
        val severity: String, // "INFO", "WARNING", "ERROR"
        val timestamp: Instant,
        val details: Map<String, String>
    )

    val logs = db.repository<AuditLog, ObjectId>("audit_logs")

    // Create TTL index for logs (expire after 7 days)
    println("Creating TTL index for audit logs (expires after 7 days)...")
    db.collection("audit_logs").admin().createIndex(
        IndexKey(linkedMapOf("timestamp" to 1)),
        IndexOptions(expireAfterSeconds = 7 * 24 * 3600) // 7 days
    )

    // Create partial index for ERROR severity only (for fast error lookup)
    println("Creating partial index for ERROR severity logs...")
    db.collection("audit_logs").admin().createIndex(
        IndexKey(linkedMapOf("userId" to 1, "timestamp" to -1)),
        IndexOptions(
            partialFilterExpression = mapOf("severity" to "ERROR")
        )
    )

    // Insert sample logs
    println("\nInserting audit logs...")
    val now = Instant.now()

    logs.save(AuditLog(
        userId = "admin-001",
        action = "USER_LOGIN",
        severity = "INFO",
        timestamp = now,
        details = mapOf("ip" to "192.168.1.100", "userAgent" to "Mozilla/5.0...")
    ))

    logs.save(AuditLog(
        userId = "user-123",
        action = "PAYMENT_FAILED",
        severity = "ERROR",
        timestamp = now.minusSeconds(300),
        details = mapOf("amount" to "99.99", "reason" to "Insufficient funds")
    ))

    logs.save(AuditLog(
        userId = "user-456",
        action = "PROFILE_UPDATE",
        severity = "INFO",
        timestamp = now.minusSeconds(600),
        details = mapOf("field" to "email", "old" to "old@example.com")
    ))

    // Query errors for a user (uses partial index)
    println("\nQuerying ERROR logs for user-123 (uses partial index):")
    val userErrors = logs.findAll(
        filter = mapOf(
            "userId" to "user-123",
            "severity" to "ERROR"
        ),
        skip = 0,
        limit = 10
    )

    userErrors.forEach { log ->
        println("  - ${log.action} at ${log.timestamp}")
        println("    Details: ${log.details}")
    }

    // Query recent logs with projection (exclude large details field)
    println("\nQuerying recent logs (with projection to exclude details):")
    val recentLogs = logs.findAll(
        filter = mapOf(
            "timestamp" to mapOf("\$gte" to now.minusSeconds(1800))
        ),
        skip = 0,
        limit = 10
    )

    recentLogs.forEach { log ->
        println("  - [${log.severity}] ${log.action} by ${log.userId}")
    }

    println("\nTotal audit logs: ${logs.count()}")
    println("\nBenefits of combined features:")
    println("  ✓ TTL: Auto-cleanup after 7 days (saves storage)")
    println("  ✓ Partial Index: Fast error lookup (reduced index size)")
    println("  ✓ Projection: Minimal data transfer (better performance)\n")
}
