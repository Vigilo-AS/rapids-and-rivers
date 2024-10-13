package no.vigilo.rapids_and_rivers_api

import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

internal class MessageProblemsTest {

    private lateinit var problems: MessageProblems

    private val message = "the message"

    @BeforeEach
    internal fun setUp() {
        problems = MessageProblems(message)
    }

    @Test
    internal fun `does not contain original message`() {
        assertFalse(problems.toString().contains(message))
        val message = "a message"
        problems.error(message)
        assertFalse(problems.toString().contains(this.message))
    }

    @Test
    internal fun `contains original message in extended report`() {
        assertFalse(problems.toExtendedReport().contains(message))
        val message = "a message"
        problems.error(message)
        assertTrue(problems.toExtendedReport().contains(this.message))
    }

    @Test
    internal fun `have no messages by default`() {
        assertFalse(problems.hasErrors())
    }

    @Test
    internal fun `severe throws`() {
        val message = "Severe error"
        assertThrows<MessageProblems.MessageException> { problems.severe(message) }
        assertTrue(problems.hasErrors())
        assertTrue(problems.toString().contains(message))
    }

    @Test
    internal fun errors() {
        val message = "Error"
        problems.error(message)
        assertTrue(problems.hasErrors())
        assertTrue(problems.toString().contains(message))
    }
}
