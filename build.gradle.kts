plugins {
    kotlin("jvm") version "1.9.22" apply false
    id("org.jetbrains.kotlin.plugin.serialization") version "1.9.22" apply false
}

allprojects {
    group = "com.sozocode"
    // 1.6.19: automatic compaction, crash-safe compaction swap, damage recovery at open,
    // data-directory lock, deleteAll(), BsonBinary Jackson module.
    version = "1.6.19"

    repositories {
        mavenCentral()
    }
}

subprojects {
    apply(plugin = "org.jetbrains.kotlin.jvm")
    apply(plugin = "java")

    // Publishable modules only; `examples` is an application.
    if (name.startsWith("minileaf-")) {
        apply(plugin = "maven-publish")

        configure<JavaPluginExtension> {
            withSourcesJar()
        }

        // Coordinates match the README: com.sozocode:mini-leaf-<module>.
        // `publishToMavenLocal` is enough for a consumer on this machine (the backend lists
        // mavenLocal() first). Publishing to Nexus needs NEXUS_URL / NEXUS_USER / NEXUS_PASSWORD
        // in the environment; credentials are never read from a file in this repository.
        configure<PublishingExtension> {
            publications {
                create<MavenPublication>("maven") {
                    groupId = "com.sozocode"
                    artifactId = project.name.replace("minileaf-", "mini-leaf-")
                    from(components["java"])
                }
            }
            val nexusUrl = System.getenv("NEXUS_URL")
            if (!nexusUrl.isNullOrBlank()) {
                repositories {
                    maven {
                        name = "nexus"
                        url = uri(nexusUrl)
                        isAllowInsecureProtocol = nexusUrl.startsWith("http://")
                        credentials {
                            username = System.getenv("NEXUS_USER")
                            password = System.getenv("NEXUS_PASSWORD")
                        }
                    }
                }
            }
        }
    }

    configure<JavaPluginExtension> {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }

    tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
        kotlinOptions {
            jvmTarget = "17"
            freeCompilerArgs = listOf("-Xjsr305=strict")
        }
    }

    tasks.withType<Test> {
        useJUnitPlatform()
    }
}
