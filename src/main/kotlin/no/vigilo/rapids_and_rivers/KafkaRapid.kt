package no.vigilo.rapids_and_rivers

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics
import no.vigilo.kafka.ConsumerProducerFactory
import no.vigilo.rapids_and_rivers_api.KeyMessageContext
import no.vigilo.rapids_and_rivers_api.RapidsConnection
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.AuthorizationException
import org.apache.kafka.common.errors.InvalidTopicException
import org.apache.kafka.common.errors.RecordBatchTooLargeException
import org.apache.kafka.common.errors.RecordTooLargeException
import org.apache.kafka.common.errors.UnknownServerException
import org.apache.kafka.common.errors.WakeupException
import org.intellij.lang.annotations.Language
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.LocalDateTime
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean

class KafkaRapid(
    factory: ConsumerProducerFactory,
    groupId: String,
    private val rapidTopic: String,
    private val meterRegistry: MeterRegistry,
    consumerProperties: Properties = Properties(),
    producerProperties: Properties = Properties(),
    private val autoCommit: Boolean = false,
    extraTopics: List<String> = emptyList(),
) : RapidsConnection(), ConsumerRebalanceListener {
    private val log = LoggerFactory.getLogger(this::class.java)

    private val running = AtomicBoolean(Stopped)
    private val ready = AtomicBoolean(false)
    private val producerClosed = AtomicBoolean(false)

    private val consumer = factory.createConsumer(groupId, consumerProperties.apply {
        if (!autoCommit) put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    }, withShutdownHook = false)
    private val producer = factory.createProducer(producerProperties, withShutdownHook = false)

    private val topics = listOf(rapidTopic) + extraTopics

    private var seekToBeginning = false

    init {
        log.info("rapid initialized, autoCommit=$autoCommit")

        KafkaClientMetrics(consumer).bindTo(meterRegistry)
        KafkaClientMetrics(producer).bindTo(meterRegistry)
    }

    fun seekToBeginning() {
        check(Stopped == running.get()) { "cannot reset consumer after rapid has started" }
        seekToBeginning = true
    }

    fun isRunning() = running.get()
    fun isReady() = isRunning() && ready.get()

    override fun publish(message: String) {
        publish(ProducerRecord(rapidTopic, message))
    }

    override fun publish(key: String, message: String?) {
        publish(ProducerRecord(rapidTopic, key, message))
    }

    override fun rapidName(): String {
        return rapidTopic
    }

    private fun publish(producerRecord: ProducerRecord<String, String>) {
        check(!producerClosed.get()) { "can't publish messages when producer is closed" }
        producer.send(producerRecord) { _, err ->
            if (err == null) return@send
            if (!isFatalError(err)) {
                log.warn("error while publishing message: ${err.message}", err)
                return@send
            }
            log.error("Shutting down rapid due to fatal error: ${err.message}", err)
            stop()
        }
    }

    override fun start() {
        log.info("starting rapid")
        if (Started == running.getAndSet(Started)) return log.info("rapid already started")
        consumeMessages()
    }

    override fun stop() {
        log.info("stopping rapid")
        if (Stopped == running.getAndSet(Stopped)) return log.info("rapid already stopped")
        notifyShutdownSignal()
        tryAndLog { producer.flush() }
        consumer.wakeup()
    }

    override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) {
        if (partitions.isEmpty()) return
        log.info("partitions assigned: $partitions")
        ensureConsumerPosition(partitions)
        notifyReady()
    }

    override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) {
        log.info("partitions revoked: $partitions")
        partitions.forEach { it.commitSync() }
        notifyNotReady()
    }

    private fun ensureConsumerPosition(partitions: Collection<TopicPartition>) {
        if (!seekToBeginning) return
        log.info("seeking to beginning for $partitions")
        consumer.seekToBeginning(partitions)
        seekToBeginning = false
    }

    private fun onRecords(records: ConsumerRecords<String, String>) {
        if (records.isEmpty) return // poll returns an empty collection in case of rebalancing
        val currentPositions = records
            .groupBy { TopicPartition(it.topic(), it.partition()) }
            .mapValues { it.value.minOf { it.offset() } }
            .toMutableMap()
        try {
            records.onEach { record ->
                if (running.get()) {
                    onRecord(record)
                    currentPositions[TopicPartition(record.topic(), record.partition())] = record.offset() + 1
                }
            }
        } catch (err: Exception) {
            log.info(
                "due to an error during processing, positions are reset to each next message (after each record that was processed OK):" +
                        currentPositions.map { "\tpartition=${it.key}, offset=${it.value}" }
                            .joinToString(separator = "\n", prefix = "\n", postfix = "\n"), err
            )
            currentPositions.forEach { (partition, offset) -> consumer.seek(partition, offset) }
            throw err
        } finally {
            consumer.commitSync(currentPositions.mapValues { (_, offset) -> offsetMetadata(offset) })
        }
    }

    private fun onRecord(record: ConsumerRecord<String, String>) {
        withMDC(recordDiganostics(record)) {
            val recordValue = record.value()
                ?: return@withMDC log.info("ignoring record with offset ${record.offset()} in partition ${record.partition()} because value is null (tombstone)")
            val context = KeyMessageContext(this, record.key())
            notifyMessage(recordValue, context, meterRegistry)
        }
    }

    private fun consumeMessages() {
        var lastException: Exception? = null
        try {
            notifyStartup()
            ready.set(true)
            consumer.subscribe(topics, this)
            while (running.get()) {
                consumer.poll(Duration.ofSeconds(1)).also {
                    withMDC(pollDiganostics(it)) {
                        onRecords(it)
                    }
                }
            }
        } catch (err: WakeupException) {
            // throw exception if we have not been told to stop
            if (running.get()) throw err
        } catch (err: Exception) {
            lastException = err
            throw err
        } finally {
            notifyShutdown()
            closeResources(lastException)
        }
    }

    private fun pollDiganostics(records: ConsumerRecords<String, String>) = mapOf(
        "rapids_poll_id" to "${UUID.randomUUID()}",
        "rapids_poll_time" to "${LocalDateTime.now()}",
        "rapids_poll_count" to "${records.count()}"
    )

    private fun recordDiganostics(record: ConsumerRecord<String, String>) = mapOf(
        "rapids_record_id" to "${UUID.randomUUID()}",
        "rapids_record_before_notify_time" to "${LocalDateTime.now()}",
        "rapids_record_produced_time" to "${record.timestamp()}",
        "rapids_record_produced_time_type" to "${record.timestampType()}",
        "rapids_record_topic" to record.topic(),
        "rapids_record_partition" to "${record.partition()}",
        "rapids_record_offset" to "${record.offset()}"
    )

    private fun TopicPartition.commitSync() {
        if (autoCommit) return
        val offset = consumer.position(this)
        log.info("committing offset offset=$offset for partition=$this")
        consumer.commitSync(mapOf(this to offsetMetadata(offset)))
    }

    private fun offsetMetadata(offset: Long): OffsetAndMetadata {
        val clientId = consumer.groupMetadata().groupInstanceId().map { "\"$it\"" }.orElse("null")

        @Language("JSON")
        val metadata = """{"time": "${LocalDateTime.now()}","groupInstanceId": $clientId}"""
        return OffsetAndMetadata(offset, metadata)
    }

    private fun closeResources(lastException: Exception?) {
        if (Started == running.getAndSet(Stopped)) {
            log.warn("stopped consuming messages due to an error", lastException)
        } else {
            log.info("stopped consuming messages after receiving stop signal")
        }
        tryAndLog(consumer::close)
        producerClosed.set(true)
        tryAndLog(producer::flush)
        tryAndLog(producer::close)
    }

    private fun tryAndLog(block: () -> Unit) {
        try {
            block()
        } catch (err: Exception) {
            log.error(err.message, err)
        }
    }

    companion object {
        private const val Stopped = false
        private const val Started = true

        private fun isFatalError(err: Exception) = when (err) {
            is InvalidTopicException,
            is RecordBatchTooLargeException,
            is RecordTooLargeException,
            is UnknownServerException,
            is AuthorizationException -> true

            else -> false
        }
    }
}
