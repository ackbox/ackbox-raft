package com.ackbox.raft.core

import com.ackbox.raft.config.NodeConfig
import com.ackbox.raft.networking.NamedChannel
import com.ackbox.raft.networking.NodeNetworking
import com.ackbox.raft.state.Metadata
import com.ackbox.raft.state.RemoteNodeState
import com.ackbox.raft.state.ReplicatedLog
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import java.time.Clock

class RemoteNodes(id: String, channels: List<NamedChannel>, private val clock: Clock) {

    private val clients = channels.map { it.id to RemoteNode(id, it, clock) }.toMap()

    fun size(): Int = clients.size

    fun appendItems(metadata: Metadata, log: ReplicatedLog): List<RemoteNodeState> {
        return runBlocking(Dispatchers.IO) {
            clients.map { (_, peer) -> async { peer.appendItems(metadata, log) } }.map { it.await() }
        }
    }

    fun requestVote(metadata: Metadata, log: ReplicatedLog): List<Boolean> {
        return runBlocking(Dispatchers.IO) {
            clients.map { (_, peer) -> async { peer.requestVote(metadata, log) } }.map { it.await() }
        }
    }

    companion object {

        fun fromConfig(config: NodeConfig, networking: NodeNetworking): RemoteNodes {
            return RemoteNodes(config.nodeId, networking.getChannels(), config.clock)
        }
    }
}
