package com.ackbox.raft.networking

import com.ackbox.raft.support.ExceptionTranslator
import com.ackbox.raft.support.NodeLogger
import io.grpc.BindableService
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.Server
import io.grpc.ServerBuilder
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder

/**
 * Interface abstracting addresses of nodes. In unit tests, a mock implementation can be used in order to
 * validate internal error cases. In system tests, an implementation allowing for simulating error conditions
 * can be used in order to tests failure conditions (e.g. packet loss, disconnections, etc).
 */
interface NodeAddress : Comparable<NodeAddress> {

    /**
     * Unique identifier for the node.
     */
    val nodeId: String

    /**
     * Converts the node address into a GRPC [Server] implementation.
     */
    fun toServer(vararg api: BindableService): Server

    /**
     * Converts the node address into a GRPC [NamedChannel] implementation.
     */
    fun toChannel(): NamedChannel

    override fun compareTo(other: NodeAddress): Int = nodeId.compareTo(other.nodeId)
}

/**
 * [NodeAddress] implementation backed by Java NIO.
 */
data class NodeNetAddress(override val nodeId: String, val host: String, val port: Int) : NodeAddress {

    private val logger: NodeLogger = NodeLogger.forNode(nodeId, NodeNetAddress::class)

    override fun toServer(vararg api: BindableService): Server {
        logger.info("Creating server on port [{}]@[{}]", port, nodeId)
        return ServerBuilder.forPort(port)
            .apply { api.forEach { addService(it) } }
            .intercept(ExceptionTranslator())
            .build()
    }

    override fun toChannel(): NamedChannel {
        val target = "$host:$port"
        logger.info("Creating channel to [{}]@[{}]", target, nodeId)
        return NamedChannel(this, createManagedChannel(target))
    }

    private fun createManagedChannel(target: String): ManagedChannel {
        return ManagedChannelBuilder.forTarget(target)
            .enableRetry()
            .usePlaintext()
            .build()
    }
}

/**
 * [NodeAddress] implementation backend by in-memory GRPC primitives.
 */
data class NodeInmemoryAddress(override val nodeId: String) : NodeAddress {

    private val logger: NodeLogger = NodeLogger.forNode(nodeId, NodeInmemoryAddress::class)

    override fun toServer(vararg api: BindableService): Server {
        logger.info("Creating in-memory server for [{}]", nodeId)
        return InProcessServerBuilder.forName(nodeId)
            .directExecutor()
            .apply { api.forEach { addService(it) } }
            .intercept(ExceptionTranslator())
            .build()
    }

    override fun toChannel(): NamedChannel {
        logger.info("Creating in-memory channel for [{}]", nodeId)
        return NamedChannel(this, createManagedChannel())
    }

    private fun createManagedChannel(): ManagedChannel {
        return InProcessChannelBuilder
            .forName(nodeId)
            .directExecutor()
            .build()
    }
}
