package com.ackbox.raft.core

import com.ackbox.raft.config.NodeConfig
import com.ackbox.raft.log.ReplicatedLog
import com.ackbox.raft.networking.NamedChannel
import com.ackbox.raft.networking.NodeNetworking
import com.ackbox.raft.state.Index
import com.ackbox.raft.state.Metadata
import com.ackbox.raft.state.RemoteNodeState
import com.ackbox.raft.statemachine.Snapshot
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import java.time.Clock

class RemoteNodes(private val config: NodeConfig, channels: List<NamedChannel>, clock: Clock) {

    private val remotes: Map<String, RemoteNode> = createRemotes(channels, clock)

    val size: Int get() = remotes.size

    fun appendItems(metadata: Metadata, log: ReplicatedLog, snapshot: Snapshot): List<RemoteNodeState> {
        return broadcastInParallel { remote -> remote.sendAppend(metadata, log, snapshot) }
    }

    fun requestVote(metadata: Metadata, log: ReplicatedLog): List<Boolean> {
        return broadcastInParallel { remote -> remote.sendVote(metadata, log) }
    }

    fun resetState(nextLogIndex: Index) {
        remotes.values.forEach { remote -> remote.resetState(nextLogIndex) }
    }

    private fun <T : Any> broadcastInParallel(consumer: suspend (RemoteNode) -> T): List<T> {
        return runBlocking(Dispatchers.IO) {
            remotes.values.map { remote -> async { consumer(remote) } }.map { it.await() }
        }
    }

    private fun createRemotes(channels: List<NamedChannel>, clock: Clock): Map<String, RemoteNode> {
        return channels.map { it.id to RemoteNode(config, it, clock) }.toMap()
    }

    companion object {

        fun fromConfig(config: NodeConfig, networking: NodeNetworking): RemoteNodes {
            return RemoteNodes(config, networking.channels, config.clock)
        }
    }
}
