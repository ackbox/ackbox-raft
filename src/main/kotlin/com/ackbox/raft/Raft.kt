package com.ackbox.raft

import com.ackbox.raft.api.ExternalNodeApi
import com.ackbox.raft.api.InternalNodeApi
import com.ackbox.raft.api.ManagementNodeApi
import com.ackbox.raft.config.NodeConfig
import com.ackbox.raft.core.LocalNode
import com.ackbox.raft.networking.NodeNetworking
import com.ackbox.raft.support.NodeLogger
import java.time.Duration

class Raft(config: NodeConfig, private val networking: NodeNetworking, private val node: LocalNode) {

    private val logger: NodeLogger = NodeLogger.from(config.nodeId, Raft::class)
    private val externalApi = ExternalNodeApi(node, config.clock)
    private val internalApi = InternalNodeApi(node, config.clock)
    private val managementApi = ManagementNodeApi(node, config.clock)

    fun start(): Raft {
        logger.info("Starting node")
        networking.start(externalApi, internalApi, managementApi)
        node.start()
        logger.info("Started node")
        return this
    }

    fun autoStop(): Raft {
        logger.info("Setting up auto-stop")
        Runtime.getRuntime().addShutdownHook(createShutdownHook())
        return this
    }

    fun stop(timeout: Duration = DEFAULT_TIMEOUT): Raft {
        logger.info("Stopping node")
        node.stop()
        networking.stop(timeout)
        logger.info("Stopped node")
        return this
    }

    fun join(): Raft {
        logger.info("Wait for node termination signal")
        networking.awaitTermination()
        logger.info("Proceeding with node termination")
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

        private val DEFAULT_TIMEOUT = Duration.ofSeconds(30)

        fun fromConfig(config: NodeConfig): Raft {
            val networking = NodeNetworking.fromConfig(config)
            val node = LocalNode.fromConfig(config, networking)
            return Raft(config, networking, node)
        }
    }
}
