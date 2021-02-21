package com.ackbox.raft.types

import com.ackbox.raft.core.RemoteNodeState
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
