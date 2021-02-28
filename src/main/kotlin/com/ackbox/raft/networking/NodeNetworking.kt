package com.ackbox.raft.networking

import com.ackbox.raft.config.NodeConfig
import com.ackbox.raft.core.StateMachine
import com.ackbox.raft.support.NodeLogger
import com.ackbox.raft.support.SERIALIZER
import com.fasterxml.jackson.module.kotlin.readValue
import io.grpc.BindableService
import io.grpc.Server
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit.MILLISECONDS
import javax.annotation.concurrent.ThreadSafe

typealias NetworkingChangedCallback = (NamedChannel) -> Unit

@ThreadSafe
class NodeNetworking(private val config: NodeConfig) : StateMachine {

    private val logger: NodeLogger = NodeLogger.forNode(config.nodeId, NodeNetworking::class)
    private val channels: ConcurrentHashMap<String, NamedChannel> = createChannels()

    private var server: Server? = null
    private var onChannelAdded: NetworkingChangedCallback? = null
    private var onChannelRemoved: NetworkingChangedCallback? = null

    fun getChannels(): List<NamedChannel> = channels.values.toList()

    fun setOnChannelAdded(callback: NetworkingChangedCallback) {
        onChannelAdded = callback
    }

    fun setOnChannelRemoved(callback: NetworkingChangedCallback) {
        onChannelRemoved = callback
    }

    fun start(vararg api: BindableService) {
        logger.info("Starting networking stack for node [{}]", config.nodeId)
        server = config.local.toServer(*api).start()
        logger.info("Starting networking stack for node [{}]", config.nodeId)
    }

    fun stop(timeout: Duration) {
        logger.info("Stopping networking stack for node [{}] with timeout [{}]", config.nodeId, timeout)
        server?.shutdown()?.awaitTermination(timeout.toMillis(), MILLISECONDS)
        channels.values.onEach { it.shutdownNow() }.forEach { it.awaitTermination(timeout.toMillis(), MILLISECONDS) }
        logger.info("Stopped networking stack for node [{}]", config.nodeId)
    }

    fun awaitTermination() {
        server?.awaitTermination()
    }

    override fun applyValue(value: ByteArray) {
        val change = NodeNetworkingChange.fromByteArray(value)
        val newChannel = when (change.type) {
            NodeNetworkingChange.Type.ADDED -> change.address.toChannel()
            NodeNetworkingChange.Type.REMOVED -> null
        }
        channels.compute(change.address.nodeId) { nodeId: String, oldChannel: NamedChannel? ->
            oldChannel?.let { onChannelRemoved?.invoke(it) }
            newChannel?.let { onChannelAdded?.invoke(it) }
            logger.info("Updated channel for node [{}]: old=[{}] and new=[{}]", nodeId, oldChannel, newChannel)
            newChannel
        }
    }

    override fun takeSnapshot(destinationPath: Path) {
        logger.info("Taking a snapshot and saving to [{}]", destinationPath)
        val filePath = Paths.get(destinationPath.toAbsolutePath().toString(), SNAPSHOT_FILENAME)
        synchronized(channels) {
            val values = channels.mapValues { it.value.address }
            SERIALIZER.writeValue(filePath.toFile(), SerializerWrapper(values))
        }
    }

    override fun restoreSnapshot(sourcePath: Path) {
        logger.info("Restoring a snapshot from [{}]", sourcePath)
        val file = Paths.get(sourcePath.toAbsolutePath().toString(), SNAPSHOT_FILENAME).toFile()
        if (!file.exists()) {
            return
        }
        synchronized(channels) {
            val values = SERIALIZER.readValue<SerializerWrapper>(file)
            channels.values.forEach { onChannelRemoved?.invoke(it) }
            channels.putAll(values.data.mapValues { it.value.toChannel() })
            channels.values.forEach { onChannelAdded?.invoke(it) }
        }
    }

    private fun createChannels(): ConcurrentHashMap<String, NamedChannel> {
        return ConcurrentHashMap(config.remotes.map { it.nodeId to it.toChannel() }.toMap())
    }

    internal data class SerializerWrapper(val data: Map<String, NodeAddress>)

    companion object {

        private const val SNAPSHOT_FILENAME: String = "networking.data.snapshot"

        fun fromConfig(config: NodeConfig): NodeNetworking = NodeNetworking(config)
    }
}
