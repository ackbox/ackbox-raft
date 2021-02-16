package com.ackbox.raft.state

import com.ackbox.raft.core.UNDEFINED_ID
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

internal class RemoteNodeStateTest {

    @Test
    fun `should be created with initial default values`() {
        val state = RemoteNodeState()

        assertEquals(UNDEFINED_ID, state.matchLogIndex)
        assertEquals(UNDEFINED_ID + 1, state.nextLogIndex)
    }
}
