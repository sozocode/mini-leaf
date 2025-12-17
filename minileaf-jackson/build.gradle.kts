plugins {
    kotlin("jvm")
}

dependencies {
    api(project(":minileaf-core"))

    // MongoDB driver for ObjectId
    implementation("org.mongodb:mongodb-driver-sync:4.11.1")

    implementation("com.fasterxml.jackson.core:jackson-databind:2.16.1")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.16.1")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.16.1")

    testImplementation("org.junit.jupiter:junit-jupiter:5.10.1")
    testImplementation("org.assertj:assertj-core:3.25.1")
}
