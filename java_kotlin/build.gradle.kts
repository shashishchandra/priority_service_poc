plugins {
    kotlin("jvm") version "2.0.21"
    application
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
}

application {
    mainClass.set("com.greyorange.ps.MainKt")
    // ZGC + large heap + throughput flags wired in via Makefile JAVA_OPTS.
    // Not set here so the Makefile controls JVM tuning explicitly (matches how
    // Go/Rust benchmarks are controlled via env vars).
}

kotlin {
    jvmToolchain(21)
}

tasks.test {
    useJUnitPlatform()
    // Run tests with the same JVM flags as the benchmark so GC behaviour is comparable.
    jvmArgs(
        "-XX:+UseZGC",
        "-XX:+ZGenerational",
        "-Xmx4g",
        "-Xms4g",
    )
}

tasks.shadowJar {
    archiveBaseName.set("priority-service-jvm")
    archiveClassifier.set("")
    archiveVersion.set("")
    mergeServiceFiles()
}
