package com.ackbox.raft

import com.ackbox.raft.api.ExternalNodeApi
import com.ackbox.raft.api.InternalNodeApi
import com.ackbox.raft.api.ManagementNodeApi
import com.ackbox.raft.config.NodeConfig
import com.ackbox.raft.core.LocalNode
import com.ackbox.raft.core.RemoteNodes
import io.grpc.Server
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.TimeUnit

class Raft(private val config: NodeConfig, private val node: LocalNode) {

    private val externalApi = ExternalNodeApi(node, config.clock)
    private val internalApi = InternalNodeApi(node, config.clock)
    private val managementApi = ManagementNodeApi(node, config.clock)

    private var server: Server? = null

    fun start(): Raft {
        LOG.info("Starting node [{}]", node.nodeId)
        server = config.local.toServer(externalApi, internalApi, managementApi).start()
        node.start()
        LOG.info("Started node [{}]", node.nodeId)
        return this
    }

    fun autoStop(): Raft {
        LOG.info("Setting up auto-stop for node [{}]", node.nodeId)
        Runtime.getRuntime().addShutdownHook(createShutdownHook())
        return this
    }

    fun stop(timeout: Duration = DEFAULT_TIMEOUT): Raft {
        LOG.info("Stopping node [{}]", node.nodeId)
        server?.shutdown()?.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS)
        node.stop(timeout)
        LOG.info("Stopped node [{}]", node.nodeId)
        return this
    }

    fun join(): Raft {
        LOG.info("Wait for termination signal for node [{}]", node.nodeId)
        server?.awaitTermination()
        LOG.info("Proceeding with termination of node [{}]", node.nodeId)
        return this
    }

    private fun createShutdownHook() = object : Thread() {
        override fun run() {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
            System.err.println("*** Shutting down gRPC server since JVM is shutting down ***")
            try {
                this@Raft.stop()
            } catch (e: InterruptedException) {
                e.printStackTrace(System.err)
                currentThread().interrupt()
            }
            System.err.println("*** Server shut down ***")
        }
    }

    fun describeState() {
        node.describeState()
    }

    companion object {

        private val LOG = LoggerFactory.getLogger(Raft::class.java)
        private val DEFAULT_TIMEOUT = Duration.ofSeconds(30)

        fun fromConfig(config: NodeConfig): Raft {
            val remotes = RemoteNodes.fromConfig(config)
            val node = LocalNode.fromConfig(config, remotes)
            return Raft(config, node)
        }
    }
}
