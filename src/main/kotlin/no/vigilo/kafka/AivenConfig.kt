package no.vigilo.kafka

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.slf4j.LoggerFactory
import java.util.*

class AivenConfig(
    private val brokers: String,
    private val trustedCertificate: String,
    private val privateKey: String,
    private val ca: String
) : Config {
    companion object {
        private val log = LoggerFactory.getLogger(AivenConfig::class.java)
        val default
            get() = AivenConfig(
                brokers = requireNotNull("${System.getenv("KAFKA_HOST")}:${System.getenv("KAFKA_PORT")}") { "Expected KAFKA_HOST and KAFKA_PORT" },
                trustedCertificate = requireNotNull(System.getenv("KAFKA_ACCESS_CERT")) { "Expected KAFKA_ACCESS_CERT" },
                privateKey = requireNotNull(System.getenv("KAFKA_ACCESS_KEY")) { "Expected KAFKA_ACCESS_KEY" },
                ca = requireNotNull(System.getenv("KAFKA_CA_CERT")) { "Expected KAFKA_CA_CERT" }
            )

//        private fun String.readFile() =
//            File(this).readText(Charsets.UTF_8)
//
//        fun envOrDefault(envName: String, defaultValue: String) =
//            System.getenv(envName) ?: defaultValue

//        // Kafka tuning parameters (defaults as per Aiven's suggestion)
//        val metadataMaxAgeMs = envOrDefault("KAFKA_METADATA_MAX_AGE_MS", "600000")
//        val requestTimeoutMs = envOrDefault("KAFKA_REQUEST_TIMEOUT_MS", "20000")
//        val connectionsMaxIdleMs = envOrDefault("KAFKA_CONNECTIONS_MAX_IDLE_MS", "300000")
//        val retryBackoffMs = envOrDefault("KAFKA_RETRY_BACKOFF_MS", "50")
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
    }

    override fun adminConfig(properties: Properties) = Properties().apply {
        putAll(kafkaBaseConfig())
        putAll(properties)
    }

    private fun kafkaBaseConfig() = Properties().apply {
        put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokers)
        put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name)
        put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PEM")
        put(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, certificateChain())
        put(SslConfigs.SSL_KEYSTORE_KEY_CONFIG, privateKey)
        put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM")
        put(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, ca)

        putAll(baseCommonClientConfigs())

    }

    private fun certificateChain() = "$privateKey\n$trustedCertificate"
}
