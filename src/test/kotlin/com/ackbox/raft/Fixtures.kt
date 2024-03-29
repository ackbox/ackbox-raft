package com.ackbox.raft

import com.ackbox.raft.config.NodeConfig
import com.ackbox.raft.networking.NodeAddress
import com.ackbox.raft.networking.NodeInmemoryAddress
import com.ackbox.raft.types.Index
import com.ackbox.raft.types.LogItem
import com.ackbox.raft.types.Term
import com.ackbox.random.krandom
import com.google.common.primitives.Longs

object Fixtures {

    private val LOCAL_NODE_ID: String = krandom()
    private val DATA_8_BYTES: ByteArray = Longs.toByteArray(krandom())

    fun createLogItem(
        type: LogItem.Type = LogItem.Type.STORE_CHANGE,
        index: Index = Index(krandom()),
        term: Term = Term(krandom()),
        data: ByteArray = DATA_8_BYTES
    ): LogItem {
        return LogItem(type, index, term, data)
    }

    fun createNodeConfig(vararg remotes: NodeAddress): NodeConfig {
        return NodeConfig(NodeInmemoryAddress(LOCAL_NODE_ID), remotes.toList())
    }
}
