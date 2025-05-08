package no.vigilo.rapids_and_rivers

import io.micrometer.core.instrument.Clock
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import io.prometheus.metrics.model.registry.PrometheusRegistry
import no.vigilo.kafka.AivenConfig
import no.vigilo.kafka.ConsumerProducerFactory
import no.vigilo.rapids_and_rivers_api.MessageContext
import no.vigilo.rapids_and_rivers_api.RandomIdGenerator
import no.vigilo.rapids_and_rivers_api.RapidsConnection
import java.net.InetAddress
import java.util.*

/**
 * RapidApplication is a wrapper around RapidsConnection that provides a simple way to create a
 * Rapid application that can be started and stopped.
 * It also provides a way to publish application events on startup, ready, not ready, shutdown signal and shutdown.
 * The application events are published to the same topic as the application messages.
 *
 * @param rapid RapidsConnection
 * @param appName String? the name of the application
 * @param instanceId String the id of the application instance
 * @param applicationEventsWithKey Boolean whether to publish application events with a key. The key is a random UUID.
 */
class RapidApplication(
    private val rapid: RapidsConnection,
    private val appName: String? = null,
    private val instanceId: String,
    private val applicationEventsWithKey: Boolean,
    registerStatusListeners: Boolean,
    registerMessageListeners: Boolean,
) : RapidsConnection(), RapidsConnection.MessageListener, RapidsConnection.StatusListener {

    init {
        Runtime.getRuntime().addShutdownHook(Thread(::shutdownHook))
        if (registerMessageListeners) rapid.register(this as MessageListener)
        if (registerStatusListeners) rapid.register(this as StatusListener)
    }

    companion object {
        private val log = org.slf4j.LoggerFactory.getLogger(RapidApplication::class.java)

        /**
         * Create a RapidApplication from environment variables.
         * The environment variables are used to create a KafkaRapid.
         * The application name is generated from the environment variables VAIS_APP_NAME, VAIS_NAMESPACE and VAIS_CLUSTER_NAME.
         * The instance id is generated from the environment variable VAIS_APP_NAME or a random UUID.
         *
         * @param env Map<String, String> the environment variables
         * @param consumerProducerFactory ConsumerProducerFactory the factory to create the KafkaRapid. Defaults to AivenConfig.default
         * @param applicationEventsWithKey Boolean whether to publish application events with a key. The key is a random UUID.
         */
        fun create(
            env: Map<String, String>,
            consumerProducerFactory: ConsumerProducerFactory = ConsumerProducerFactory(AivenConfig.default),
            applicationEventsWithKey: Boolean = false,
            registerStatusListeners: Boolean = true,
            registerMessageListeners: Boolean = true,
        ): RapidsConnection {
            val meterRegistry =
                PrometheusMeterRegistry(PrometheusConfig.DEFAULT, PrometheusRegistry.defaultRegistry, Clock.SYSTEM)

            val kafkaRapid = createDefaultKafkaRapidFromEnv(
                factory = consumerProducerFactory,
                meterRegistry = meterRegistry,
                env = env
            )

            return RapidApplication(
                rapid = kafkaRapid,
                appName = generateAppName(env),
                instanceId = generateInstanceId(env),
                applicationEventsWithKey = applicationEventsWithKey,
                registerStatusListeners = registerStatusListeners,
                registerMessageListeners = registerMessageListeners
            )
        }

        private fun generateInstanceId(env: Map<String, String>): String {
            if (env.containsKey("VAIS_APP_NAME")) return InetAddress.getLocalHost().hostName
            return UUID.randomUUID().toString()
        }

        private fun generateAppName(env: Map<String, String>): String? {
            val appName =
                env["VAIS_APP_NAME"] ?: return log.info("not generating app name because VAIS_APP_NAME not set")
                    .let { null }
            val namespace =
                env["VAIS_NAMESPACE"] ?: return log.info("not generating app name because VAIS_NAMESPACE not set")
                    .let { null }
            val cluster =
                env["VAIS_CLUSTER_NAME"] ?: return log.info("not generating app name because VAIS_CLUSTER_NAME not set")
                    .let { null }
            return "$appName-$cluster-$namespace"
        }
    }

    override fun start() {
        rapid.start()
    }

    override fun stop() {
        rapid.stop()
    }

    override fun publish(message: String) {
        rapid.publish(message)
    }

    override fun publish(key: String, message: String?) {
        rapid.publish(key, message)
    }

    override fun rapidName(): String {
        return rapid.rapidName()
    }

    override fun onMessage(message: String, context: MessageContext, metrics: MeterRegistry) {
        notifyMessage(message, context, metrics)
    }

    private fun shutdownHook() {
        log.info("received shutdown signal, stopping app")
        stop()
    }

    override fun onStartup(rapidsConnection: RapidsConnection) {
        publishApplicationEvent(rapidsConnection, "application_up")
        notifyStartup()
    }

    override fun onReady(rapidsConnection: RapidsConnection) {
        publishApplicationEvent(rapidsConnection, "application_ready")
        notifyReady()
    }

    override fun onNotReady(rapidsConnection: RapidsConnection) {
        publishApplicationEvent(rapidsConnection, "application_not_ready")
        notifyNotReady()
    }

    override fun onShutdownSignal(rapidsConnection: RapidsConnection) {
        publishApplicationEvent(rapidsConnection, "application_stop")
        notifyShutdownSignal()
    }

    override fun onShutdown(rapidsConnection: RapidsConnection) {
        publishApplicationEvent(rapidsConnection, "application_down")
        notifyShutdown()
    }

    private fun publishApplicationEvent(rapidsConnection: RapidsConnection, event: String) {
        applicationEvent(event)?.also {
            log.info("publishing $event event for app_name=$appName, instance_id=$instanceId")
            try {
                if (applicationEventsWithKey) {
                    rapidsConnection.publish(RandomIdGenerator.Default.generateId(), it)
                } else {
                    rapidsConnection.publish(it)
                }
            } catch (err: Exception) {
                log.info("failed to publish event: {}", err.message, err)
            }
        }
    }

    private fun applicationEvent(event: String): String? {
        if (appName == null) return null
        val packet = JsonMessage.newMessage(
            event, mapOf(
                "app_name" to appName,
                "instance_id" to instanceId
            )
        )
        return packet.toJson()
    }
}