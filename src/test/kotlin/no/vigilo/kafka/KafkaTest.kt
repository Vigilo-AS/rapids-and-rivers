package no.vigilo.kafka

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.OffsetSpec
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.config.SaslConfigs
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.util.*
import java.util.concurrent.CountDownLatch
import kotlin.random.Random

class KafkaTest {
    private companion object {
        private val kafkaContainer = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"))
    }

    @Test
    fun factory() {
        kafkaContainer.e2eTest { testTopic, producer, consumer, adminClient ->
            val key = "key"
            val value = "message"

            val recordMeta = producer.send(ProducerRecord(testTopic, key, value)).get()

            assertUntil(Duration.ofSeconds(10)) {
                val records = consumer.poll(Duration.ofMillis(200))
                assertEquals(1, records.count())
                val record = records.single()
                assertEquals(key, record.key())
                assertEquals(value, record.value())
            }

            val partitions = adminClient.getPartitions(testTopic)
            val expectedOffsets = partitions.associateWith {
                if (it.partition() == recordMeta.partition()) recordMeta.offset() + 1 else 0
            }
            val actualOffsets = adminClient.findOffsets(partitions, OffsetSpec.latest())
            assertEquals(expectedOffsets, actualOffsets)
        }
    }

    @Test
    fun polling() {
        kafkaContainer.e2eTest { testTopic, producer, consumer, adminClient ->
            val key = "key"
            val value = "message"

            producer.send(ProducerRecord(testTopic, key, value)).get()

            val expectedPollCount = 10
            val latch = CountDownLatch(expectedPollCount + 1)
            val until = {
                latch.countDown()
                latch.count > 0
            }

            val pollResults = mutableListOf<ConsumerRecords<String, String>>()
            consumer.poll(until) { records ->
                pollResults.add(records)
            }

            assertEquals(expectedPollCount, pollResults.size)
            assertEquals(1, pollResults.sumOf { it.count() })
        }
    }
}

private fun assertUntil(duration: Duration, test: () -> Unit) {
    val endTime = System.currentTimeMillis() + duration.toMillis()
    lateinit var lastAssertionError: Throwable
    while (System.currentTimeMillis() < endTime) {
        try {
            return test()
        } catch (assertionError: AssertionError) {
            lastAssertionError = assertionError
        }
    }
    throw lastAssertionError
}

fun <R> KafkaContainer.e2eTest(block: (String, KafkaProducer<String, String>, KafkaConsumer<String, String>, AdminClient) -> R): R {
    if (!isRunning) start()
    val config = KafkaTestConfig(this)
    val factory = ConsumerProducerFactory(config)
    return factory.createConsumer("integration-test-${Random.nextInt()}", withShutdownHook = false).use { consumer ->
        val testTopic = "integration-topic-${Random.nextInt()}"
        consumer.subscribe(listOf(testTopic))
        factory.createProducer().use { producer ->
            block(testTopic, producer, consumer, factory.adminClient())
        }
    }
}

private class KafkaTestConfig(private val kafkaContainer: KafkaContainer) : Config {
    override fun producerConfig(properties: Properties): Properties {
        return properties.apply {
            connectionConfig(this)
            put(ProducerConfig.ACKS_CONFIG, "all")
            put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
            put(ProducerConfig.LINGER_MS_CONFIG, "0")
            put(ProducerConfig.RETRIES_CONFIG, "0")
        }
    }

    override fun consumerConfig(groupId: String, properties: Properties): Properties {
        return properties.apply {
            connectionConfig(this)
            put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        }
    }

    override fun adminConfig(properties: Properties): Properties {
        return properties.apply {
            connectionConfig(this)
        }
    }

    private fun connectionConfig(properties: Properties) = properties.apply {
        put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.bootstrapServers)
        put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT")
        put(SaslConfigs.SASL_MECHANISM, "PLAIN")
    }
}