// See https://gradle.org and https://github.com/gradle/kotlin-dsl

// Apply the java plugin to add support for Java
plugins {
    java
    application
}

repositories {
    jcenter()
}

version = "1.0.1"

dependencies {
    // Our beloved one-nio
    compile("ru.odnoklassniki:one-nio:1.0.2")

    // Annotations for better code documentation
    compile("com.intellij:annotations:12.0")

    // JUnit 5
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.3.1")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.3.1")

    // Xodus
    compile("org.jetbrains.xodus:xodus-openAPI:1.2.3")
    compile("org.jetbrains.xodus:xodus-environment:1.2.3")

    // JUnit Jupiter test framework
    testCompile("org.junit.jupiter:junit-jupiter-api:5.3.1")

    // Guava for tests
    testCompile("com.google.guava:guava:23.1-jre")

    compile("org.slf4j:slf4j-api:1.7.+")
    compile("ch.qos.logback:logback-classic:1.1.3")
}

tasks {
    "test"(Test::class) {
        maxHeapSize = "128m"
        useJUnitPlatform()
    }
}

application {
    // Define the main class for the application
    mainClassName = "ru.mail.polis.Cluster"

    // And limit Xmx
    applicationDefaultJvmArgs = listOf("-Xmx128m")
}
