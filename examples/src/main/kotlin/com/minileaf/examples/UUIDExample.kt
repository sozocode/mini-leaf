package com.minileaf.examples

import com.minileaf.core.Minileaf
import com.minileaf.core.config.MinileafConfig
import com.minileaf.kotlin.repositoryWithUUID
import java.nio.file.Paths
import java.util.UUID

/**
 * Example demonstrating UUID support in Minileaf.
 *
 * This example shows how to use UUID instead of ObjectId for document identifiers.
 */

data class UUIDProduct(
    var _id: UUID? = null,
    val name: String,
    val price: Double,
    val category: String,
    val inStock: Boolean
)

data class UUIDCustomer(
    var _id: UUID? = null,
    val email: String,
    val firstName: String,
    val lastName: String,
    val phoneNumber: String?
)

fun main() {
    println("=== Minileaf UUID Example ===\n")

    val db = Minileaf.open(
        MinileafConfig(
            dataDir = Paths.get("./minileaf-uuid-demo"),
            memoryOnly = false
        )
    )

    try {
        demoProductsWithUUID(db)
        demoCustomersWithUUID(db)
    } finally {
        db.close()
        println("\n=== Demo Complete ===")
    }
}

fun demoProductsWithUUID(db: Minileaf) {
    println("### Products with UUID IDs ###\n")

    // Get a repository with UUID as the ID type
    val products = db.repositoryWithUUID<UUIDProduct>("uuid_products")

    // Create products with auto-generated UUIDs
    println("Creating products...")
    val laptop = products.save(UUIDProduct(
        name = "Gaming Laptop",
        price = 1299.99,
        category = "Electronics",
        inStock = true
    ))
    println("  Created: ${laptop.name} (ID: ${laptop._id})")

    val mouse = products.save(UUIDProduct(
        name = "Wireless Mouse",
        price = 29.99,
        category = "Accessories",
        inStock = true
    ))
    println("  Created: ${mouse.name} (ID: ${mouse._id})")

    val keyboard = products.save(UUIDProduct(
        name = "Mechanical Keyboard",
        price = 89.99,
        category = "Accessories",
        inStock = false
    ))
    println("  Created: ${keyboard.name} (ID: ${keyboard._id})")

    // Create a product with a specific UUID
    val specificId = UUID.fromString("550e8400-e29b-41d4-a716-446655440000")
    val monitor = products.save(UUIDProduct(
        _id = specificId,
        name = "4K Monitor",
        price = 599.99,
        category = "Electronics",
        inStock = true
    ))
    println("  Created: ${monitor.name} (Custom ID: ${monitor._id})")

    // Find by UUID
    println("\nFinding product by UUID...")
    val found = products.findById(laptop._id!!)
    if (found.isPresent) {
        println("  Found: ${found.get().name} - \$${found.get().price}")
    }

    // Find all products
    println("\nAll products:")
    val allProducts = products.findAll()
    allProducts.forEach { product ->
        val stockStatus = if (product.inStock) "In Stock" else "Out of Stock"
        println("  - ${product.name} (${product.category}): \$${product.price} - $stockStatus")
        println("    UUID: ${product._id}")
    }

    println("\nTotal products: ${products.count()}\n")
}

fun demoCustomersWithUUID(db: Minileaf) {
    println("### Customers with UUID IDs ###\n")

    // Get a repository with UUID as the ID type
    val customers = db.repositoryWithUUID<UUIDCustomer>("uuid_customers")

    // Create customers
    println("Creating customers...")
    val customer1 = customers.save(UUIDCustomer(
        email = "john.doe@example.com",
        firstName = "John",
        lastName = "Doe",
        phoneNumber = "+1-555-1234"
    ))
    println("  Created: ${customer1.firstName} ${customer1.lastName} (ID: ${customer1._id})")

    val customer2 = customers.save(UUIDCustomer(
        email = "jane.smith@example.com",
        firstName = "Jane",
        lastName = "Smith",
        phoneNumber = null
    ))
    println("  Created: ${customer2.firstName} ${customer2.lastName} (ID: ${customer2._id})")

    val customer3 = customers.save(UUIDCustomer(
        email = "bob.wilson@example.com",
        firstName = "Bob",
        lastName = "Wilson",
        phoneNumber = "+1-555-5678"
    ))
    println("  Created: ${customer3.firstName} ${customer3.lastName} (ID: ${customer3._id})")

    // Query customers
    println("\nQuerying customers with phone numbers...")
    val customersWithPhones = customers.findAll(
        filter = mapOf("phoneNumber" to mapOf("\$exists" to true)),
        skip = 0,
        limit = 10
    )
    println("  Found ${customersWithPhones.size} customers with phone numbers:")
    customersWithPhones.forEach { customer ->
        println("    - ${customer.firstName} ${customer.lastName}: ${customer.phoneNumber}")
    }

    // Update a customer
    println("\nUpdating customer...")
    val updated = customer2.copy(phoneNumber = "+1-555-9999")
    customers.save(updated)
    println("  Updated ${updated.firstName} ${updated.lastName} with phone: ${updated.phoneNumber}")

    // Delete a customer
    println("\nDeleting customer...")
    val deleted = customers.deleteById(customer3._id!!)
    if (deleted.isPresent) {
        println("  Deleted: ${deleted.get().firstName} ${deleted.get().lastName}")
    }

    // Final count
    println("\nTotal customers remaining: ${customers.count()}")

    // List all customers
    println("\nAll remaining customers:")
    val allCustomers = customers.findAll()
    allCustomers.forEach { customer ->
        println("  - ${customer.email} (${customer.firstName} ${customer.lastName})")
        println("    UUID: ${customer._id}")
        println("    Phone: ${customer.phoneNumber ?: "N/A"}")
    }

    println()
}
