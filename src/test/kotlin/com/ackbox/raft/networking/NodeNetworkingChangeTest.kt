package com.ackbox.raft.networking

import com.ackbox.raft.networking.NodeNetworkingChange.Type
import com.ackbox.random.krandom
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

internal class NodeNetworkingChangeTest {

    @Test
    fun `should be able to properly serialize remove changes`() {
        val change = NodeNetworkingChange(Type.REMOVED, NodeInmemoryAddress(NODE_ID))

        assertEquals(change, NodeNetworkingChange.fromByteArray(change.toByteArray()))
    }

    @Test
    fun `should be able to properly serialize add changes`() {
        val change = NodeNetworkingChange(Type.ADDED, NodeInmemoryAddress(NODE_ID))

        assertEquals(change, NodeNetworkingChange.fromByteArray(change.toByteArray()))
    }

    companion object {

        private val NODE_ID: String = krandom()
    }
}
