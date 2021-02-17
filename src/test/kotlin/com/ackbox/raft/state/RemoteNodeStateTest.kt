package com.ackbox.raft.state

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

internal class RemoteNodeStateTest {

    @Test
    fun `should be created with initial default values`() {
        val state = RemoteNodeState()

        assertEquals(Index.UNDEFINED, state.matchLogIndex)
        assertEquals(Index.UNDEFINED.incremented(), state.nextLogIndex)
    }
}
