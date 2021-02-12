package com.ackbox.raft.networking

import com.ackbox.raft.config.NodeConfig
import com.ackbox.raft.support.NodeLogger
import io.grpc.BindableService
import io.grpc.Server
import java.time.Duration
import java.util.concurrent.TimeUnit

class NodeNetworking(private val config: NodeConfig) {

    private val logger = NodeLogger.from(config.nodeId, NodeNetworking::class)
    private val channels: List<NamedChannel> = config.targets.map { it.toChannel() }
    private var server: Server? = null

    fun getChannels(): List<NamedChannel> = channels

    fun start(vararg api: BindableService) {
        server = config.local.toServer(*api).start()
    }

    fun stop(timeout: Duration) {
        logger.info("Stopping networking stack for node [{}] with timeout [{}]", config.nodeId, timeout)
        server?.shutdown()?.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS)
        channels.onEach { it.shutdownNow().awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS) }
        logger.info("Stopped networking stack for node [{}]", config.nodeId)
    }

    fun awaitTermination() {
        server?.awaitTermination()
    }

    companion object {

        fun fromConfig(config: NodeConfig): NodeNetworking = NodeNetworking(config)
    }
}
