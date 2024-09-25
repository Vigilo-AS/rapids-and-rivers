package no.vigilo.kafka

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.slf4j.LoggerFactory
import java.io.File
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

        private fun String.readFile() =
            File(this).readText(Charsets.UTF_8)
    }

    init {
        check(brokers.isNotEmpty())
        log.info(certificateChain())
    }

    override fun producerConfig(properties: Properties) = Properties().apply {
        putAll(kafkaBaseConfig())
        put(ProducerConfig.ACKS_CONFIG, "1")
        put(ProducerConfig.LINGER_MS_CONFIG, "0")
        put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
        putAll(properties)
    }

    override fun consumerConfig(groupId: String, properties: Properties) = Properties().apply {
        putAll(kafkaBaseConfig())
        put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
        putAll(properties)
        put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    }

    override fun adminConfig(properties: Properties) = Properties().apply {
        putAll(kafkaBaseConfig())
        putAll(properties)
    }

    private fun kafkaBaseConfig() = Properties().apply {
        put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokers)
        put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name)
        put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PEM");
        put(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, certificateChain());
        put(SslConfigs.SSL_KEYSTORE_KEY_CONFIG, privateKey);
        put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM");
        put(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, ca);
    }

    private fun certificateChain() = "$privateKey\n$trustedCertificate"
}
