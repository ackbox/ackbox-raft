package com.ackbox.raft.core

import com.ackbox.raft.config.NodeConfig
import com.ackbox.raft.log.ReplicatedLog
import com.ackbox.raft.networking.NodeAddress
import com.ackbox.raft.statemachine.Snapshot
import com.ackbox.raft.support.NodeLogger
import com.ackbox.raft.support.UnknownNodeException
import com.ackbox.raft.types.Index
import com.ackbox.raft.types.Metadata
import com.ackbox.raft.types.RemoteNodeState
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import javax.annotation.concurrent.ThreadSafe

@ThreadSafe
class RemoteNodes(private val config: NodeConfig) {

    private val logger: NodeLogger = NodeLogger.from(config.nodeId, RemoteNodes::class)
    private val remotes: ConcurrentHashMap<String, RemoteNode> = ConcurrentHashMap()

    val remotesCount: Int get() = remotes.size

    fun start() {
        config.remotes.asSequence()
            .map { address -> address.toChannel() }
            .forEach { channel -> remotes[channel.id] = RemoteNode(config, channel) }
    }

    fun stop(timeout: Duration) {
        remotes.values.map { remoteNode -> remoteNode.channel }
            .onEach { channel -> channel.runCatching { shutdownNow() } }
            .onEach { channel -> channel.runCatching { awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS) } }
    }

    fun activateNode(address: NodeAddress) {
        activateNode(address) {}
    }

    fun <T : Any?> activateNode(address: NodeAddress, snapshot: Snapshot? = null, callback: () -> T): T {
        val nodeId = address.nodeId
        val channel = address.toChannel()
        try {
            logger.info("Creating a new remote for node [{}]", nodeId)
            val remoteNode = RemoteNode(config, channel)
            snapshot?.let {
                logger.info("Sending snapshot to remote node [{}]", nodeId)
                remoteNode.sendSnapshot(snapshot)
            }
            logger.info("Activating remote node [{}]", nodeId)
            remotes[channel.id] = remoteNode
        } catch (e: Exception) {
            remotes.remove(nodeId)
            channel.runCatching { shutdownNow() }
            throw e
        }
        return callback()
    }

    fun deactivateNode(nodeId: String) {
        logger.info("Deactivating remote for node [{}]", nodeId)
        val remoteNode = remotes.remove(nodeId)
        logger.info("Node [{}] was mapped to remote [{}]", nodeId, remoteNode)
        remoteNode?.channel?.runCatching { shutdownNow() }
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

    companion object {

        fun fromConfig(config: NodeConfig): RemoteNodes = RemoteNodes(config)
    }
}
