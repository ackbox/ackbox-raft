package com.ackbox.raft.networking

import com.ackbox.raft.config.NodeConfig
import com.ackbox.raft.support.NodeLogger
import io.grpc.BindableService
import io.grpc.Server
import java.time.Duration
import java.util.concurrent.TimeUnit

class NodeNetworking(private val config: NodeConfig) {

    private val logger: NodeLogger = NodeLogger.from(config.nodeId, NodeNetworking::class)
    private var server: Server? = null

    val channels: List<NamedChannel> = config.remotes.map { it.toChannel() }

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
