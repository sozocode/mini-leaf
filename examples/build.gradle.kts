plugins {
    kotlin("jvm")
    application
}

dependencies {
    implementation(project(":minileaf-core"))
    implementation(project(":minileaf-jackson"))
    implementation(project(":minileaf-kotlin"))

    implementation("org.mongodb:mongodb-driver-sync:4.11.1")
    implementation("ch.qos.logback:logback-classic:1.4.14")
}

application {
    mainClass.set("com.minileaf.examples.BasicExampleKt")
}

// Task to run advanced features example
tasks.register<JavaExec>("runAdvanced") {
    group = "application"
    description = "Run the advanced features example"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.minileaf.examples.AdvancedFeaturesExampleKt")
}

// Task to run UUID example
tasks.register<JavaExec>("runUUID") {
    group = "application"
    description = "Run the UUID example"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.minileaf.examples.UUIDExampleKt")
}
