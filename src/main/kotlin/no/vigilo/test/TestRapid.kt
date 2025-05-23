package no.vigilo.test

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import no.vigilo.rapids_and_rivers_api.RapidsConnection

class TestRapid(private val meterRegistry: MeterRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)) : RapidsConnection() {
    private companion object {
        private val objectMapper = jacksonObjectMapper()
            .registerModule(JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    }

    private val messages = mutableListOf<Pair<String?, String?>>()
    val inspector get() = RapidInspector(messages.toList())

    fun reset() {
        messages.clear()
    }

    fun sendTestMessage(message: String) {
        notifyMessage(message, this, meterRegistry)
    }

    override fun publish(message: String) {
        messages.add(null to message)
    }

    override fun publish(key: String, message: String?) {
        messages.add(key to message)
    }

    override fun rapidName(): String {
        return "testRapid"
    }

    override fun start() {}
    override fun stop() {}

    class RapidInspector(private val messages: List<Pair<String?, String?>>) {
        private val jsonMessages = mutableMapOf<Int, JsonNode>()
        val size get() = messages.size

        fun key(index: Int) = messages[index].first
        fun message(index: Int) = jsonMessages.getOrPut(index) { messages[index].second?.let { objectMapper.readTree(it) } ?: objectMapper.nullNode() }
        fun field(index: Int, field: String) = requireNotNull(message(index).path(field).takeUnless { it.isMissingNode || it.isNull }) {
            "Message does not contain field '$field'"
        }
    }
}
