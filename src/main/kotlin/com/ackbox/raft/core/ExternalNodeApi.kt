package com.ackbox.raft.core

import com.ackbox.raft.api.GetReply
import com.ackbox.raft.api.GetRequest
import com.ackbox.raft.api.PublicNodeGrpcKt.PublicNodeCoroutineImplBase
import com.ackbox.raft.api.SetReply
import com.ackbox.raft.api.SetRequest
import com.ackbox.raft.core.LeaderNode.Get
import com.ackbox.raft.core.LeaderNode.Set
import com.ackbox.raft.support.CommitIndexMismatchException
import com.ackbox.raft.support.LockNotAcquiredException
import com.ackbox.raft.support.NodeLogger
import com.ackbox.raft.support.NotLeaderException
import com.google.protobuf.ByteString
import java.nio.ByteBuffer
import java.time.Clock

/**
 * Public Raft node API implementation. This is the API exposed to external application wanting to communicate
 * with the nodes in the cluster. The main methods exposed here are "set" and "get", which will ensure entries
 * can be safely set and retrieved from nodes in the cluster.
 */
class ExternalNodeApi(private val node: LeaderNode, private val clock: Clock) : PublicNodeCoroutineImplBase() {

    private val logger: NodeLogger = NodeLogger.from(node.nodeId, ExternalNodeApi::class)

    override suspend fun set(request: SetRequest): SetReply {
        logger.debug("Received SET request [{}]", request)
        return try {
            val input = Set.Input(listOf(request.entry.toByteArray()))
            val output = node.setItem(input)
            createSuccessSetReply(output.leaderId)
        } catch (e: NotLeaderException) {
            logger.warn("Received SET request while not leader", e.knownLeaderId, e)
            createFailureSetReply(e.knownLeaderId, SetReply.Status.NOT_LEADER)
        } catch (e: CommitIndexMismatchException) {
            logger.warn("Commit index mismatch for SET request", e)
            createFailureSetReply(e.leaderId, SetReply.Status.COMMIT_ERROR)
        } catch (e: LockNotAcquiredException) {
            logger.warn("Unable to complete request since node is busy", e)
            createFailureSetReply(null, SetReply.Status.PROCESSING)
        } catch (e: Exception) {
            logger.error("Unable to complete request due unknown error", e)
            createFailureSetReply(null, SetReply.Status.UNKNOWN)
        }
    }

    override suspend fun get(request: GetRequest): GetReply {
        logger.debug("Received GET request [{}]", request)
        return try {
            val input = Get.Input(request.key)
            val output = node.getItem(input)
            createSuccessGetReply(output.leaderId, output.data)
        } catch (e: NotLeaderException) {
            logger.warn("Received GET request while not leader", e.knownLeaderId, e)
            createFailureGetReply(e.knownLeaderId, GetReply.Status.NOT_LEADER)
        } catch (e: LockNotAcquiredException) {
            logger.warn("Unable to complete request since node is busy", e)
            createFailureGetReply(null, GetReply.Status.PROCESSING)
        } catch (e: Exception) {
            logger.error("Unable to complete request due unknown error", e)
            createFailureGetReply(null, GetReply.Status.UNKNOWN)
        }
    }

    private fun createSuccessSetReply(leaderId: String?): SetReply {
        return SetReply.newBuilder().apply {
            this.timestamp = clock.millis()
            leaderId?.let { this.leaderId = it }
            this.status = SetReply.Status.SUCCESS
        }.build()
    }

    private fun createFailureSetReply(leaderId: String?, status: SetReply.Status): SetReply {
        return SetReply.newBuilder().apply {
            this.timestamp = clock.millis()
            leaderId?.let { this.leaderId = it }
            this.status = status
        }.build()
    }

    private fun createSuccessGetReply(leaderId: String?, data: ByteBuffer?): GetReply {
        return GetReply.newBuilder().apply {
            this.timestamp = clock.millis()
            leaderId?.let { this.leaderId = it }
            this.entry = data?.let { ByteString.copyFrom(it) }
            this.status = GetReply.Status.SUCCESS
        }.build()
    }

    private fun createFailureGetReply(leaderId: String?, status: GetReply.Status): GetReply {
        return GetReply.newBuilder().apply {
            this.timestamp = clock.millis()
            leaderId?.let { this.leaderId = it }
            this.status = status
        }.build()
    }
}
