package com.ackbox.raft.api

import com.ackbox.raft.api.ManagementNodeGrpcKt.ManagementNodeCoroutineImplBase
import com.ackbox.raft.core.LeaderNode
import com.ackbox.raft.networking.NodeNetAddress
import com.ackbox.raft.support.LockNotAcquiredException
import com.ackbox.raft.support.NodeLogger
import com.ackbox.raft.support.NotLeaderException
import java.time.Clock

/**
 * Raft cluster management API implementation.
 */
class ManagementNodeApi(private val node: LeaderNode, private val clock: Clock) : ManagementNodeCoroutineImplBase() {

    private val logger: NodeLogger = NodeLogger.forNode(node.nodeId, ManagementNodeApi::class)

    override suspend fun addNode(request: AddNodeRequest): AddNodeReply {
        logger.debug("Received add node request [{}]", request)
        return try {
            val address = NodeNetAddress(request.nodeId, request.host, request.port)
            val output = node.addNode(LeaderNode.AddNode.Input(request.requestId, address))
            createSuccessAddNodeReply(output.leaderId)
        } catch (e: NotLeaderException) {
            logger.warn("Received add node request while not leader=[{}]", e.knownLeaderId, e)
            createFailureAddNodeReply(e.knownLeaderId, AddNodeReply.Status.NOT_LEADER)
        } catch (e: LockNotAcquiredException) {
            logger.warn("Unable to complete request since node is busy", e)
            createFailureAddNodeReply(null, AddNodeReply.Status.PROCESSING)
        } catch (e: Exception) {
            logger.error("Unable to complete request due unknown error", e)
            createFailureAddNodeReply(null, AddNodeReply.Status.UNKNOWN)
        }
    }

    override suspend fun removeNode(request: RemoveNodeRequest): RemoveNodeReply {
        logger.debug("Received remove node request [{}]", request)
        return try {
            val address = NodeNetAddress(request.nodeId, request.host, request.port)
            val output = node.removeNode(LeaderNode.RemoveNode.Input(request.requestId, address))
            createSuccessRemoveNodeReply(output.leaderId)
        } catch (e: NotLeaderException) {
            logger.warn("Received remove node request while not leader=[{}]", e.knownLeaderId, e)
            createFailureRemoveNodeReply(e.knownLeaderId, RemoveNodeReply.Status.NOT_LEADER)
        } catch (e: LockNotAcquiredException) {
            logger.warn("Unable to complete request since node is busy", e)
            createFailureRemoveNodeReply(null, RemoveNodeReply.Status.PROCESSING)
        } catch (e: Exception) {
            logger.error("Unable to complete request due unknown error", e)
            createFailureRemoveNodeReply(null, RemoveNodeReply.Status.UNKNOWN)
        }
    }

    private fun createSuccessAddNodeReply(leaderId: String?): AddNodeReply {
        return AddNodeReply.newBuilder().apply {
            this.timestamp = clock.millis()
            leaderId?.let { this.leaderId = it }
            this.status = AddNodeReply.Status.SUCCESS
        }.build()
    }

    private fun createFailureAddNodeReply(leaderId: String?, status: AddNodeReply.Status): AddNodeReply {
        return AddNodeReply.newBuilder().apply {
            this.timestamp = clock.millis()
            leaderId?.let { this.leaderId = it }
            this.status = status
        }.build()
    }

    private fun createSuccessRemoveNodeReply(leaderId: String?): RemoveNodeReply {
        return RemoveNodeReply.newBuilder().apply {
            this.timestamp = clock.millis()
            leaderId?.let { this.leaderId = it }
            this.status = RemoveNodeReply.Status.SUCCESS
        }.build()
    }

    private fun createFailureRemoveNodeReply(leaderId: String?, status: RemoveNodeReply.Status): RemoveNodeReply {
        return RemoveNodeReply.newBuilder().apply {
            this.timestamp = clock.millis()
            leaderId?.let { this.leaderId = it }
            this.status = status
        }.build()
    }
}
