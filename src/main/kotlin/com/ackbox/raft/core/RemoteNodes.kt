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

class RemoteNodes(localNodeId: String, channels: List<NamedChannel>, clock: Clock) {

    private val remotes = channels.map { it.id to RemoteNode(localNodeId, it, clock) }.toMap()

    fun size(): Int = remotes.size

    fun appendItems(metadata: Metadata, log: ReplicatedLog): List<RemoteNodeState> {
        return runBlocking(Dispatchers.IO) {
            remotes.map { (_, remote) -> async { remote.appendItems(metadata, log) } }.map { it.await() }
        }
    }

    fun requestVote(metadata: Metadata, log: ReplicatedLog): List<Boolean> {
        return runBlocking(Dispatchers.IO) {
            remotes.map { (_, remote) -> async { remote.requestVote(metadata, log) } }.map { it.await() }
        }
    }

    companion object {

        fun fromConfig(config: NodeConfig, networking: NodeNetworking): RemoteNodes {
            return RemoteNodes(config.nodeId, networking.getChannels(), config.clock)
        }
    }
}
