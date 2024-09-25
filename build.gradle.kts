plugins {
    kotlin("jvm") version "2.0.20"
    `java-library`
    java
    `maven-publish`
}

group = "no.vigilo"
version = properties["version"] ?: "local-build"

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

java {
    withSourcesJar()
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

//                licenses {
//                    license {
//                        name.set("MIT License")
//                        url.set("https://opensource.org/licenses/MIT")
//                    }
//                }

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