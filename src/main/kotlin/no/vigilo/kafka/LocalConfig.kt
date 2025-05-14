package no.vigilo.kafka

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import java.io.File
import java.util.*

class LocalConfig(
    private val brokers: String,
) : Config {
    companion object {
        val default
            get() = LocalConfig(
                brokers = requireNotNull("${System.getenv("KAFKA_HOST")}:${System.getenv("KAFKA_PORT")}") { "Expected KAFKA_HOST and KAFKA_PORT" },

                )

        private fun String.readFile() =
            File(this).readText(Charsets.UTF_8)
    }

    init {
        check(brokers.isNotEmpty())
    }

    override fun producerConfig(properties: Properties) = Properties().apply {
        putAll(kafkaBaseConfig())
        putAll(baseProducerConfig())
        putAll(properties)
    }

    override fun consumerConfig(groupId: String, properties: Properties) = Properties().apply {
        putAll(kafkaBaseConfig())
        putAll(baseConsumerConfig())
        putAll(properties)
        put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    }

    override fun adminConfig(properties: Properties) = Properties().apply {
        putAll(kafkaBaseConfig())
        putAll(properties)
    }

    private fun kafkaBaseConfig() = Properties().apply {
        put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokers)
        putAll(baseCommonClientConfigs())
    }
}
