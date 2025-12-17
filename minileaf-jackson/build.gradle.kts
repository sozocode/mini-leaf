plugins {
    kotlin("jvm")
    `maven-publish`
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

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])

            groupId = "com.sozocode"
            artifactId = "mini-leaf-jackson"
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
