plugins {
    kotlin("jvm")
    `maven-publish`
}

dependencies {
    // MongoDB driver for ObjectId
    implementation("org.mongodb:mongodb-driver-sync:4.11.1")

    // Jackson for JSON processing
    implementation("com.fasterxml.jackson.core:jackson-databind:2.16.1")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.16.1")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.16.1")

    // Kotlin coroutines
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.7.3")

    // Logging
    implementation("io.github.microutils:kotlin-logging-jvm:3.0.5")
    implementation("ch.qos.logback:logback-classic:1.4.14")

    // Testing
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.1")
    testImplementation("org.junit.jupiter:junit-jupiter-params:5.10.1")
    testImplementation("io.mockk:mockk:1.13.9")
    testImplementation("org.assertj:assertj-core:3.25.1")
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])

            groupId = "com.sozocode"
            artifactId = "mini-leaf-core"
            version = project.version.toString()
        }
    }

    repositories {
        maven {
            name = "nexus"
            url = if (version.toString().endsWith("SNAPSHOT")) {
                uri("http://nexus.sozocode.com/repository/maven-snapshots/")
            } else {
                uri("http://nexus.sozocode.com/repository/maven-releases/")
            }
            isAllowInsecureProtocol = true
            credentials {
                username = project.findProperty("nexusUsername") as String? ?: ""
                password = project.findProperty("nexusPassword") as String? ?: ""
            }
        }
    }
}
