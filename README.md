# Rapids and rivers

Library for easily creating microservices that use the concept of rapids and rivers by [@fredgeorge](https://github.com/fredgeorge/). For more
information, you can watch this video https://vimeo.com/79866979.

This library is mainly nicked for [@navikt](https://github.com/navikt/). See the following for more information:
- [Kafka library](https://github.com/navikt/tbd-libs/tree/main/kafka)
- [Rapids and rivers](https://github.com/navikt/tbd-libs/tree/main/rapids-and-rivers)
- [Rapids and rivers api](https://github.com/navikt/tbd-libs/tree/main/rapids-and-rivers-api)
- [Rapids and rivers test](https://github.com/navikt/tbd-libs/tree/main/rapids-and-rivers-test)
- [Rapids and rivers app](https://github.com/navikt/rapids-and-rivers)

## Installation
Add the repository to your `build.gradle.kts` file:
```kotlin
repositories {
    val githubPassword: String? by project
    ...
    maven {
        url = uri("https://maven.pkg.github.com/vigilo-as/rapids-and-rivers")
        credentials {
            username = "x-access-token"
            password = githubPassword ?: System.getenv("GRADLE_TOKEN")
        }
    }
}
``` 

Add the dependency to your `build.gradle.kts` file:
```kotlin
implementation("no.vigilo:rapids-and-rivers:<latest version>")
```
You can find latest version [here](https://github.com/Vigilo-AS/rapids-and-rivers/packages/2264175)

## Usage
### Setup a SpringRapidApplication
```kotlin
@Configuration
class RapidConfig {
    @Bean
    fun springRapidApplication(props: Props) = SpringRapidApplication.create(
        localKafka = props.localKafka,
        applicationEventsWithKeys = props.applicationEventsWithKeys,
    )
}
```
### Setup a River
```kotlin
class RiverExample(rapid: SpringRapidApplication) : River.PacketListener {

    init {
        River(rapid.connection()).apply {
            // Setup your validation rules here
            validate { it.requireValue("@event_name", "entity") }
            validate { it.requireValue("entity_type", "hr.person") }
            
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        TODO("Not yet implemented")
    }
}
```