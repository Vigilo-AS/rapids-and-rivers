package no.vigilo.rapids_and_rivers

import no.vigilo.test.TestRapid
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.test.assertTrue

internal class KeyAndNullableMessage {

    private lateinit var testRapid: TestRapid

    @BeforeEach
    fun setup() {
        testRapid = TestRapid()
    }

    @Test
    fun `we can publish a message with a key and a null as message`() {
        testRapid.publish("key", null)

        assertTrue { testRapid.inspector.size == 1 }
        assertTrue { testRapid.inspector.message(0).isNull }
    }

    @Test
    fun `we can publish a message with a key and a non-null as message`() {
        testRapid.publish("key", "{}")

        assertTrue { testRapid.inspector.size == 1 }
        assertTrue { testRapid.inspector.message(0).isContainerNode }
    }
}