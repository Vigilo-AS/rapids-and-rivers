package no.vigilo.rapids_and_rivers

import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import no.vigilo.kafka.Config
import no.vigilo.kafka.ConsumerProducerFactory
import no.vigilo.rapids_and_rivers_api.MessageContext
import no.vigilo.rapids_and_rivers_api.MessageProblems
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.SaslConfigs
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.*
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.shaded.org.awaitility.Awaitility.await
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.time.LocalDateTime
import java.util.*
import java.util.concurrent.TimeUnit.SECONDS

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class RapidIntegrationTest {
    private val objectMapper = jacksonObjectMapper()
        .registerModule(JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    private val consumerId = "test-app"

    private val testTopic = "a-test-topic"
    private val anotherTestTopic = "a-test-topic"

    private val kafkaContainer = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.2.1"))

    private lateinit var kafkaProducer: Producer<String, String>
    private lateinit var kafkaConsumer: Consumer<String, String>
    private lateinit var kafkaAdmin: AdminClient

    private val localConfig = LocalKafkaConfig(kafkaContainer)
    private val factory: ConsumerProducerFactory = ConsumerProducerFactory(localConfig)
    private lateinit var rapid: KafkaRapid
    private lateinit var rapidJob: Job

    @BeforeAll
    internal fun setup() {
        kafkaContainer.start()

        kafkaProducer = factory.createProducer()
        kafkaConsumer = factory.createConsumer("integration-test")
        kafkaAdmin = factory.adminClient()
        kafkaConsumer.subscribe(listOf(testTopic))
    }

    @AfterAll
    internal fun teardown() {
        kafkaConsumer.unsubscribe()
        kafkaConsumer.close()
        kafkaProducer.close()
        kafkaContainer.stop()
    }

    @DelicateCoroutinesApi
    @BeforeEach
    internal fun start() {
        rapid = createTestRapid()
        rapid.startNonBlocking()

        await("wait until the rapid has started")
            .atMost(20, SECONDS)
            .until(rapid::isRunning)
    }

    @AfterEach
    internal fun stop() {
        rapid.stop()
        runBlocking { rapidJob.cancelAndJoin() }
    }

    @Test
    fun `no effect calling start multiple times`() {
        assertDoesNotThrow { rapid.start() }
        assertTrue(rapid.isRunning())
    }

    @Test
    fun `can stop`() {
        rapid.stop()
        assertFalse(rapid.isRunning())
        assertDoesNotThrow { rapid.stop() }
    }

    @Test
    fun `should stop on errors`() {
        rapid.register { _, _, _ -> throw RuntimeException() }

        await("wait until the rapid stops")
            .atMost(20, SECONDS)
            .until {
                kafkaProducer.send(ProducerRecord(testTopic, UUID.randomUUID().toString()))
                !rapid.isRunning()
            }
    }

    @DelicateCoroutinesApi
    @Test
    fun `in case of exception, the offset committed is the erroneous record`() {
        ensureRapidIsActive()

        // stop rapid so we can queue up records
        rapid.stop()
        runBlocking { rapidJob.cancelAndJoin() }

        val offsets = (0..100).map {
            val key = UUID.randomUUID().toString()
            kafkaProducer.send(ProducerRecord(testTopic, key, "{\"test_message_index\": $it}"))
                    .get()
                    .offset()
        }

        val failOnMessage = 50
        val expectedOffset = offsets[failOnMessage]
        var readFailedMessage = false

        rapid = createTestRapid()
        River(rapid)
            .validate { it.requireKey("test_message_index") }
            .onSuccess { packet: JsonMessage, _: MessageContext ->
                val index = packet["test_message_index"].asInt()
                println("Read test_message_index=$index")
                if (index == failOnMessage) {
                    readFailedMessage = true
                    throw RuntimeException("an unexpected error happened")
                }
            }

        rapid.startNonBlocking()

        await("wait until the failed message has been read")
                .atMost(20, SECONDS)
                .until { readFailedMessage }
        await("wait until the rapid stops")
                .atMost(20, SECONDS)
                .until { !rapid.isRunning() }

        val actualOffset = kafkaAdmin
                .listConsumerGroupOffsets(consumerId)
                ?.partitionsToOffsetAndMetadata()
                ?.get()
                ?.getValue(TopicPartition(testTopic, 0))
                ?: fail { "was not able to fetch committed offset for consumer $consumerId" }
        val metadata = actualOffset.metadata() ?: fail { "expected metadata to be present in OffsetAndMetadata" }
        assertEquals(expectedOffset, actualOffset.offset())
        assertTrue(objectMapper.readTree(metadata).has("groupInstanceId"))
        assertDoesNotThrow { LocalDateTime.parse(objectMapper.readTree(metadata).path("time").asText()) }
    }

    private fun ensureRapidIsActive() {
        val readMessages = mutableListOf<JsonMessage>()
        River(rapid).onSuccess { packet: JsonMessage, _: MessageContext -> readMessages.add(packet) }

        await("wait until the rapid has read the test message")
                .atMost(5, SECONDS)
                .until {
                    rapid.publish("{\"foo\": \"bar\"}")
                    readMessages.size >= 1
                }
    }

    @Test
    fun `ignore tombstone messages`() {
        val serviceId = "my-service"
        val eventName = "heartbeat"

        testRiver(eventName, serviceId)
        val recordMetadata = waitForReply(testTopic, serviceId, eventName, null)

        val offsets = kafkaAdmin
            .listConsumerGroupOffsets(consumerId)
            ?.partitionsToOffsetAndMetadata()
            ?.get()
            ?: fail { "was not able to fetch committed offset for consumer $consumerId" }
        val actualOffset = offsets.getValue(TopicPartition(recordMetadata.topic(), recordMetadata.partition()))
        val metadata = actualOffset.metadata() ?: fail { "expected metadata to be present in OffsetAndMetadata" }
        assertTrue(actualOffset.offset() >= recordMetadata.offset())
        assertTrue(objectMapper.readTree(metadata).has("groupInstanceId"))
        assertDoesNotThrow { LocalDateTime.parse(objectMapper.readTree(metadata).path("time").asText()) }
    }

    @Test
    fun `read and produce message`() {
        val serviceId = "my-service"
        val eventName = "heartbeat"
        val value = "{ \"@event\": \"$eventName\" }"

        testRiver(eventName, serviceId)
        val recordMetadata = waitForReply(testTopic, serviceId, eventName, value)

        val offsets = kafkaAdmin
            .listConsumerGroupOffsets(consumerId)
            ?.partitionsToOffsetAndMetadata()
            ?.get()
            ?: fail { "was not able to fetch committed offset for consumer $consumerId" }
        val actualOffset = offsets.getValue(TopicPartition(recordMetadata.topic(), recordMetadata.partition()))
        val metadata = actualOffset.metadata() ?: fail { "expected metadata to be present in OffsetAndMetadata" }
        assertTrue(actualOffset.offset() >= recordMetadata.offset())
        assertTrue(objectMapper.readTree(metadata).has("groupInstanceId"))
        assertDoesNotThrow { LocalDateTime.parse(objectMapper.readTree(metadata).path("time").asText()) }
    }

    @DelicateCoroutinesApi
    @Test
    fun `seek to beginning`() {
        val readMessages = mutableListOf<JsonMessage>()
        River(rapid).onSuccess { packet: JsonMessage, _: MessageContext -> readMessages.add(packet) }

        var producedMessages = 0
        await("wait until the rapid has read the test message")
            .atMost(5, SECONDS)
            .until {
                rapid.publish("{\"foo\": \"bar\"}")
                producedMessages += 1
                readMessages.size >= 1
            }

        rapid.stop()
        runBlocking { rapidJob.cancelAndJoin() }

        readMessages.clear()

        rapid = createTestRapid()
        River(rapid).onSuccess { packet: JsonMessage, _: MessageContext -> readMessages.add(packet) }
        rapid.seekToBeginning()
        rapid.startNonBlocking()

        await("wait until the rapid has read more than one message")
            .atMost(20, SECONDS)
            .until { readMessages.size >= producedMessages }
    }

    @Test
    fun `read from others topics and produce to rapid topic`() {
        val serviceId = "my-service"
        val eventName = "heartbeat"
        val value = "{ \"@event\": \"$eventName\" }"

        testRiver(eventName, serviceId)
        waitForReply(anotherTestTopic, serviceId, eventName, value)
    }

    @DelicateCoroutinesApi
    private fun KafkaRapid.startNonBlocking() {
        rapidJob = GlobalScope.launch {
            try {
                this@startNonBlocking.start()
            } catch (err: Exception) {
                // swallow
            }
        }
    }

    private fun createTestRapid(): KafkaRapid {
        return KafkaRapid(factory, consumerId, testTopic, PrometheusMeterRegistry(PrometheusConfig.DEFAULT), extraTopics = listOf(anotherTestTopic))
    }

    private fun testRiver(eventName: String, serviceId: String) {
        River(rapid).apply {
            validate { it.requireValue("@event", eventName) }
            validate { it.forbid("service_id") }
            register(object : River.PacketListener {
                override fun onPacket(packet: JsonMessage, context: MessageContext) {
                    packet["service_id"] = serviceId
                    context.publish(packet.toJson())
                }

                override fun onError(problems: MessageProblems, context: MessageContext) {}
            })
        }
    }

    private fun waitForReply(topic: String, serviceId: String, eventName: String, event: String?): RecordMetadata {
        val sentMessages = mutableListOf<String>()
        val key = UUID.randomUUID().toString()
        val recordMetadata = kafkaProducer.send(ProducerRecord(topic, key, event)).get(5000, SECONDS)
        sentMessages.add(key)
        await("wait until we get a reply")
            .atMost(20, SECONDS)
            .until {
                kafkaConsumer.poll(Duration.ZERO).forEach {
                    if (!sentMessages.contains(it.key())) return@forEach
                    if (it.key() != key) return@forEach
                    return@until true
                }
                return@until false
            }
        return recordMetadata
    }

}

private class LocalKafkaConfig(private val kafkaContainer: KafkaContainer) : Config {
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