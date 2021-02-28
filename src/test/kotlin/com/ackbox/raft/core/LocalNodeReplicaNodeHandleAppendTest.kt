package com.ackbox.raft.core

import com.ackbox.raft.config.NodeConfig
import com.ackbox.raft.networking.NodeInmemoryAddress
import com.ackbox.raft.networking.NodeNetworking
import com.ackbox.raft.support.UnknownNodeException
import com.ackbox.raft.types.Index
import com.ackbox.raft.types.Partition
import com.ackbox.raft.types.Term
import com.ackbox.random.krandom
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.UUID

internal class LocalNodeReplicaNodeHandleAppendTest {

    @Test
    fun `should reject request from unknown node`() {
        val config = createNodeConfig()
        val node = LocalNode.fromConfig(config, NodeNetworking(config))

        assertThrows<UnknownNodeException> { runBlocking { node.handleAppend(createAppendInput()) } }
    }

    private fun createAppendInput(): ReplicaNode.Append.Input {
        return ReplicaNode.Append.Input(
            UUID.randomUUID().toString(),
            Partition(1),
            UNKNOWN_NODE_ID,
            Term.UNDEFINED,
            Index.UNDEFINED,
            Term.UNDEFINED,
            Index.UNDEFINED,
            emptyList()
        )
    }

    private fun createNodeConfig(): NodeConfig {
        return NodeConfig(NodeInmemoryAddress(LOCAL_NODE_ID), listOf(NodeInmemoryAddress(REMOTE_NODE_ID)))
    }

    companion object {

        private val LOCAL_NODE_ID: String = krandom()
        private val REMOTE_NODE_ID: String = krandom()
        private val UNKNOWN_NODE_ID: String = krandom()
    }
}
