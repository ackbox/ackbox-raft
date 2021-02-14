package com.ackbox.raft

import com.ackbox.raft.config.NodeConfig
import com.ackbox.raft.core.ExternalNodeApi
import com.ackbox.raft.core.InternalNodeApi
import com.ackbox.raft.core.LocalNode
import com.ackbox.raft.core.RemoteNodes
import com.ackbox.raft.networking.NodeNetworking
import org.slf4j.LoggerFactory
import java.time.Clock
import java.time.Duration

class Raft(private val node: LocalNode, private val networking: NodeNetworking, clock: Clock) {

    private val externalApi = ExternalNodeApi(node, clock)
    private val internalApi = InternalNodeApi(node, clock)

    fun start(): Raft {
        LOG.info("Starting node [{}]", node.nodeId)
        networking.start(externalApi, internalApi)
        node.start()
        LOG.info("Started node [{}]", node.nodeId)
        return this
    }

    fun autoStop(): Raft {
        Runtime.getRuntime().addShutdownHook(createShutdownHook())
        return this
    }

    fun stop(timeout: Duration = DEFAULT_TIMEOUT): Raft {
        LOG.info("Stopping node [{}]", node.nodeId)
        networking.stop(timeout)
        node.stop()
        LOG.info("Stopped node [{}]", node.nodeId)
        return this
    }

    fun join(): Raft {
        LOG.info("Wait for termination signal for node [{}]", node.nodeId)
        networking.awaitTermination()
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
            val networking = NodeNetworking.fromConfig(config)
            val peers = RemoteNodes.fromConfig(config, networking)
            val node = LocalNode.fromConfig(config, peers)
            return Raft(node, networking, config.clock)
        }
    }
}
