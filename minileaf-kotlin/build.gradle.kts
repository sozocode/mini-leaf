plugins {
    kotlin("jvm")
}

dependencies {
    api(project(":minileaf-core"))
    api(project(":minileaf-jackson"))

    // MongoDB driver for ObjectId
    implementation("org.mongodb:mongodb-driver-sync:4.11.1")

    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.7.3")

    testImplementation("org.junit.jupiter:junit-jupiter:5.10.1")
    testImplementation("org.assertj:assertj-core:3.25.1")
}
