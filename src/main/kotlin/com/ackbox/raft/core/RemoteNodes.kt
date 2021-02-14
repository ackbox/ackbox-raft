package com.ackbox.raft.core

import com.ackbox.raft.config.NodeConfig
import com.ackbox.raft.log.ReplicatedLog
import com.ackbox.raft.networking.NamedChannel
import com.ackbox.raft.networking.NodeNetworking
import com.ackbox.raft.state.Metadata
import com.ackbox.raft.state.RemoteNodeState
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import java.time.Clock

class RemoteNodes(private val config: NodeConfig, channels: List<NamedChannel>, clock: Clock) {

    private val remotes: Map<String, RemoteNode> =
        channels.map { it.id to RemoteNode(config.nodeId, it, clock) }.toMap()

    fun size(): Int = remotes.size

    fun appendItems(metadata: Metadata, log: ReplicatedLog): List<RemoteNodeState> {
        return runBlocking(Dispatchers.IO) {
            remotes.map { (_, remote) ->
                callAsync { remote.appendItems(metadata, log) }
            }.mapNotNull { it.await() }
        }
    }

    fun requestVote(metadata: Metadata, log: ReplicatedLog): List<Boolean> {
        return runBlocking(Dispatchers.IO) {
            remotes.map { (_, remote) ->
                callAsync { remote.requestVote(metadata, log) }
            }.mapNotNull { it.await() }
        }
    }

    private suspend fun <T : Any> callAsync(call: () -> T): Deferred<T?> = coroutineScope {
        async { withTimeoutOrNull(config.getRemoteRpcTimeout().toMillis()) {  call.invoke() } }
    }

    companion object {

        fun fromConfig(config: NodeConfig, networking: NodeNetworking): RemoteNodes {
            return RemoteNodes(config, networking.getChannels(), config.clock)
        }
    }
}
