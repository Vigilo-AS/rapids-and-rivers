plugins {
    kotlin("jvm") version "2.0.20"
    `java-library`
}

group = "no.vigilo"
version = "1.0-SNAPSHOT"

val slf4jVersion = "2.0.16"
val micrometerRegistryPrometheusVersion = "1.13.4"
val jacksonVersion = "2.17.2"
val testcontainersVersion = "1.20.1"
val kafkaVersion = "3.8.0"
val kafkaTestcontainerVersion = "1.20.1"

repositories {
    mavenCentral()
}

dependencies {
    api("org.apache.kafka:kafka-clients:$kafkaVersion")
    api("com.fasterxml.jackson.module:jackson-module-kotlin:$jacksonVersion")
    api("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:$jacksonVersion")
    api("org.slf4j:slf4j-api:$slf4jVersion")
    api("io.micrometer:micrometer-registry-prometheus:$micrometerRegistryPrometheusVersion")


    testImplementation(kotlin("test"))
    testImplementation("org.testcontainers:kafka:$kafkaTestcontainerVersion")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.9.0-RC.2")


}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(17)
}