val slf4jVersion = "2.0.16"
val micrometerRegistryPrometheusVersion = "1.14.1"
val jacksonVersion = "2.18.2"
val testcontainersVersion = "1.20.1"
val kafkaVersion = "3.9.0"
val kafkaTestcontainerVersion = "1.20.1"

plugins {
    kotlin("jvm") version "2.0.20"
    `java-library`
    java
    `maven-publish`
}

group = "no.vigilo"
version = version


repositories {
    mavenCentral()
}

dependencies {
    api("org.apache.kafka:kafka-clients:$kafkaVersion")
    api("com.fasterxml.jackson.module:jackson-module-kotlin:$jacksonVersion")
    api("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:$jacksonVersion")
    api("org.slf4j:slf4j-api:$slf4jVersion")
    api("io.micrometer:micrometer-registry-prometheus:$micrometerRegistryPrometheusVersion")
    api("org.springframework.boot:spring-boot:3.4.0")

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

java {
    withSourcesJar()
    withJavadocJar()
}

val githubUser: String? by project
val githubPassword: String? by project

publishing {
    repositories {
        maven {
            url = uri("https://maven.pkg.github.com/vigilo-as/rapids-and-rivers")
            credentials {
                username = githubUser
                password = githubPassword
            }
        }
    }
    publications {
        create<MavenPublication>("mavenJava") {

            pom {
                name.set("rapids-rivers")
                description.set("Rapids and Rivers")
                url.set("https://github.com/vigilo-as/rapids-and-rivers")

                scm {
                    connection.set("scm:git:https://github.com/vigilo-as/rapids-and-rivers.git")
                    developerConnection.set("scm:git:https://github.com/vigilo-as/rapids-and-rivers.git")
                    url.set("https://github.com/vigilo-as/rapids-and-rivers")
                }
            }
            from(components["java"])
        }
    }
}