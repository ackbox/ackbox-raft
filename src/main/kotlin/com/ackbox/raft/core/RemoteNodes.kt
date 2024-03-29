package com.ackbox.raft.core

import com.ackbox.raft.config.NodeConfig
import com.ackbox.raft.log.ReplicatedLog
import com.ackbox.raft.networking.NamedChannel
import com.ackbox.raft.networking.NodeAddress
import com.ackbox.raft.support.NodeLogger
import com.ackbox.raft.support.UnknownNodeException
import com.ackbox.raft.types.Index
import com.ackbox.raft.types.Metadata
import com.google.common.annotations.VisibleForTesting
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import java.util.concurrent.ConcurrentHashMap
import javax.annotation.concurrent.ThreadSafe

@ThreadSafe
class RemoteNodes(private val config: NodeConfig, channels: List<NamedChannel>) {

    private val logger: NodeLogger = NodeLogger.forNode(config.nodeId, RemoteNodes::class)
    private val remotes: ConcurrentHashMap<String, RemoteNode> = createRemotes(channels)

    val remotesCount: Int get() = remotes.size

    fun activateNode(channel: NamedChannel) {
        logger.info("Creating a new remote for node [{}]", channel.id)
        val remote = remotes.computeIfAbsent(channel.id) { createRemoteNode(channel) }
        logger.info("Node [{}] is now mapped remote [{}]", channel.id, remote)
    }

    fun deactivateNode(channel: NamedChannel) {
        logger.info("Deactivating remote for node [{}]", channel.id)
        val remoteNode = remotes.remove(channel.id)
        logger.info("Node [{}] was mapped to remote [{}]", channel.id, remoteNode)
    }

    fun appendItems(requestId: String, metadata: Metadata, log: ReplicatedLog, snapshot: Snapshot): List<RemoteNodeState> {
        return broadcastInParallel { remote -> remote.sendAppend(requestId, metadata, log, snapshot) }
    }

    fun requestVote(requestId: String, metadata: Metadata, log: ReplicatedLog): List<Boolean> {
        return broadcastInParallel { remote -> remote.sendVote(requestId, metadata, log) }
    }

    fun sendSnapshotTo(address: NodeAddress, requestId: String, metadata: Metadata, snapshot: Snapshot) {
        val channel = address.toChannel()
        try {
            val remoteNode = createRemoteNode(channel)
            remoteNode.sendSnapshot(requestId, metadata, snapshot)
        } finally {
            channel.runCatching { shutdownNow() }
        }
    }

    fun resetState(nextLogIndex: Index) {
        remotes.values.forEach { remote -> remote.resetState(nextLogIndex) }
    }

    fun ensureValidRemote(issuerNodeId: String) {
        if (!remotes.containsKey(issuerNodeId)) {
            throw UnknownNodeException(issuerNodeId)
        }
    }

    private fun <T : Any> broadcastInParallel(consumer: (RemoteNode) -> T): List<T> {
        return runBlocking(Dispatchers.IO) {
            remotes.values.filter { it.remoteId != config.nodeId }
                .map { remote -> async { consumer(remote) } }.map { it.await() }
        }
    }

    private fun createRemotes(channels: List<NamedChannel>): ConcurrentHashMap<String, RemoteNode> {
        val remotes = channels.map { channel -> channel.id to createRemoteNode(channel) }
        return ConcurrentHashMap(remotes.toMap())
    }

    @VisibleForTesting
    internal fun createRemoteNode(channel: NamedChannel): RemoteNode {
        return RemoteChannelNode(config, channel)
    }

    companion object {

        fun fromConfig(config: NodeConfig, channels: List<NamedChannel>): RemoteNodes {
            return RemoteNodes(config, channels)
        }
    }
}
