package no.vigilo.kafka


import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import java.util.*

interface Config {
    fun producerConfig(properties: Properties): Properties
    fun consumerConfig(groupId: String, properties: Properties): Properties
    fun adminConfig(properties: Properties): Properties

    private fun envOrDefault(envName: String, defaultValue: String) =
        System.getenv(envName) ?: defaultValue

    fun baseProducerConfig(): Properties = Properties().apply {
        put(ProducerConfig.ACKS_CONFIG, "all")
        put(ProducerConfig.LINGER_MS_CONFIG, "5")
        put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5")
        put(ProducerConfig.RETRIES_CONFIG, "10")
        put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
        put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4")
        put(ProducerConfig.BATCH_SIZE_CONFIG, "65536")
    }

    fun baseConsumerConfig() : Properties = Properties().apply {
        put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
        put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "2000")
        put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "65536")
        put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "100")
        put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")

    }
    fun baseCommonClientConfigs(): Properties = Properties().apply {
        val metadataMaxAgeMs = envOrDefault("KAFKA_METADATA_MAX_AGE_MS", "600000")
        val requestTimeoutMs = envOrDefault("KAFKA_REQUEST_TIMEOUT_MS", "20000")
        val connectionsMaxIdleMs = envOrDefault("KAFKA_CONNECTIONS_MAX_IDLE_MS", "300000")
        val retryBackoffMs = envOrDefault("KAFKA_RETRY_BACKOFF_MS", "50")

        // Tunables
        put(CommonClientConfigs.METADATA_MAX_AGE_CONFIG, metadataMaxAgeMs)
        put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs)
        put(CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG, connectionsMaxIdleMs)
        put(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG, retryBackoffMs)
    }
}
