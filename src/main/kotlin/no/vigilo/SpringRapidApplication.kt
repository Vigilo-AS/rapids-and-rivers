package no.vigilo

import no.vigilo.kafka.AivenConfig
import no.vigilo.kafka.ConsumerProducerFactory
import no.vigilo.kafka.LocalConfig
import no.vigilo.rapids_and_rivers.RapidApplication
import no.vigilo.rapids_and_rivers_api.RapidsConnection
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener

class SpringRapidApplication(
    private val localKafka: Boolean,
    applicationEventsWithKeys: Boolean,
    registerStatusListeners: Boolean,
    registerMessageListeners: Boolean,
    private val rapid: RapidsConnection = RapidApplication.create(
        System.getenv(),
        ConsumerProducerFactory(if (localKafka) LocalConfig.default else AivenConfig.default),
        applicationEventsWithKey = applicationEventsWithKeys,
        registerStatusListeners = registerStatusListeners,
        registerMessageListeners = registerMessageListeners,
    )
) : Runnable {

    //private val rapid = RapidApplication.create(System.getenv(), ConsumerProducerFactory(config()), applicationEventsWithKeys)

    companion object {
        /**
         * Create a new SpringRapidApplication instance.
         * @param localKafka Boolean whether to use a local Kafka instance. Defaults to false.
         * @param applicationEventsWithKeys Boolean whether to publish application events with a key. The key is a random UUID.
         * @param registerStatusListeners Boolean whether to register status listeners. Defaults to true.
         * @param registerMessageListeners Boolean whether to register message listeners. Defaults to true.
         * @return SpringRapidApplication
         */
        fun create(
            localKafka: Boolean,
            applicationEventsWithKeys: Boolean,
            registerStatusListeners: Boolean,
            registerMessageListeners: Boolean
        ) =
            SpringRapidApplication(
                localKafka = localKafka,
                applicationEventsWithKeys = applicationEventsWithKeys,
                registerStatusListeners = registerStatusListeners,
                registerMessageListeners = registerMessageListeners,
            )

        /**
         * Create a new SpringRapidApplication instance with rapid connection.
         * @param localKafka Boolean whether to use a local Kafka instance. Defaults to false.
         * @param applicationEventsWithKeys Boolean whether to publish application events with a key. The key is a random UUID.
         * @param rapid RapidsConnection the rapid connection to use.
         * @param registerStatusListeners Boolean whether to register status listeners. Defaults to true.
         * @param registerMessageListeners Boolean whether to register message listeners. Defaults to true.
         * @return SpringRapidApplication
         */
        fun create(
            localKafka: Boolean,
            applicationEventsWithKeys: Boolean,
            rapid: RapidsConnection,
            registerStatusListeners: Boolean,
            registerMessageListeners: Boolean
        ) =
            SpringRapidApplication(
                localKafka = localKafka,
                applicationEventsWithKeys = applicationEventsWithKeys,
                rapid = rapid,
                registerStatusListeners = registerStatusListeners,
                registerMessageListeners = registerMessageListeners,
            )
    }

    @EventListener(ApplicationReadyEvent::class)
    fun afterPropertiesSet() {
        Thread(this).start()
    }

    fun publish(message: String) {
        rapid.publish(message)
    }

    fun publish(key: String, message: String?) {
        rapid.publish(key, message)
    }

    fun register(listener: RapidsConnection.MessageListener) {
        rapid.register(listener)
    }

    fun connection() = rapid


    override fun run() {
        rapid.start()
    }

}