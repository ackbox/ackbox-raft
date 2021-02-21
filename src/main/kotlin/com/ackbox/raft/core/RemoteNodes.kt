package com.ackbox.raft.core

import com.ackbox.raft.config.NodeConfig
import com.ackbox.raft.log.ReplicatedLog
import com.ackbox.raft.networking.NamedChannel
import com.ackbox.raft.support.NodeLogger
import com.ackbox.raft.support.UnknownNodeException
import com.ackbox.raft.types.Index
import com.ackbox.raft.types.Metadata
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import java.util.concurrent.ConcurrentHashMap
import javax.annotation.concurrent.ThreadSafe

@ThreadSafe
class RemoteNodes(private val config: NodeConfig, channels: List<NamedChannel>) {

    private val logger: NodeLogger = NodeLogger.from(config.nodeId, RemoteNodes::class)
    private val remotes: ConcurrentHashMap<String, RemoteNode> = createRemotes(channels)

    val remotesCount: Int get() = remotes.size

    fun activateNode(channel: NamedChannel) {
        logger.info("Creating a new remote for node [{}]", channel.id)
        val remote = remotes.computeIfAbsent(channel.id) { RemoteNode(config, channel) }
        logger.info("Node [{}] is now mapped remote [{}]", channel.id, remote)
    }

    fun deactivateNode(channel: NamedChannel) {
        logger.info("Deactivating remote for node [{}]", channel.id)
        val remoteNode = remotes.remove(channel.id)
        logger.info("Node [{}] was mapped to remote [{}]", channel.id, remoteNode)
    }

    fun appendItems(metadata: Metadata, log: ReplicatedLog, snapshot: Snapshot): List<RemoteNodeState> {
        return broadcastInParallel { remote -> remote.sendAppend(metadata, log, snapshot) }
    }

    fun requestVote(metadata: Metadata, log: ReplicatedLog): List<Boolean> {
        return broadcastInParallel { remote -> remote.sendVote(metadata, log) }
    }

    fun resetState(nextLogIndex: Index) {
        remotes.values.forEach { remote -> remote.resetState(nextLogIndex) }
    }

    fun ensureValidRemote(issuerNodeId: String) {
        if (!remotes.containsKey(issuerNodeId)) {
            throw UnknownNodeException(issuerNodeId)
        }
    }

    private fun <T : Any> broadcastInParallel(consumer: suspend (RemoteNode) -> T): List<T> {
        return runBlocking(Dispatchers.IO) {
            remotes.values.map { remote -> async { consumer(remote) } }.map { it.await() }
        }
    }

    private fun createRemotes(channels: List<NamedChannel>): ConcurrentHashMap<String, RemoteNode> {
        val remotes = channels.map { channel -> channel.id to RemoteNode(config, channel) }
        return ConcurrentHashMap(remotes.toMap())
    }

    companion object {

        fun fromConfig(config: NodeConfig, channels: List<NamedChannel>): RemoteNodes {
            return RemoteNodes(config, channels)
        }
    }
}
