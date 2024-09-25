package no.vigilo.rapids_and_rivers

import no.vigilo.rapids_and_rivers_api.MessageContext


internal class JsonMessageContext(
    private val rapidsConnection: MessageContext,
    private val packet: JsonMessage
) : MessageContext {
    override fun publish(message: String) {
        rapidsConnection.publish(populateStandardFields(message))
    }

    override fun publish(key: String, message: String) {
        rapidsConnection.publish(key, populateStandardFields(message))
    }

    private fun populateStandardFields(message: String) =
        JsonMessage.populateStandardFields(packet, message)

    override fun rapidName(): String {
        return rapidsConnection.rapidName()
    }
}