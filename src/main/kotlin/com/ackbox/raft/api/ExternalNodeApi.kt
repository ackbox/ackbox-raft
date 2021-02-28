package com.ackbox.raft.api

import com.ackbox.raft.api.ExternalNodeGrpcKt.ExternalNodeCoroutineImplBase
import com.ackbox.raft.core.LeaderNode
import com.ackbox.raft.core.LeaderNode.GetEntry
import com.ackbox.raft.core.LeaderNode.SetEntry
import com.ackbox.raft.support.CommitIndexMismatchException
import com.ackbox.raft.support.LockNotAcquiredException
import com.ackbox.raft.support.NodeLogger
import com.ackbox.raft.support.NotLeaderException
import com.google.protobuf.ByteString
import java.nio.ByteBuffer
import java.time.Clock

/**
 * External Raft node API implementation. This is the API exposed to external application wanting to communicate with
 * the nodes in the cluster. The main methods exposed here are "set" and "get", which will ensure entries can be safely
 * set and retrieved from nodes in the cluster.
 */
class ExternalNodeApi(private val node: LeaderNode, private val clock: Clock) : ExternalNodeCoroutineImplBase() {

    private val logger: NodeLogger = NodeLogger.forNode(node.nodeId, ExternalNodeApi::class)

    override suspend fun setEntry(request: SetEntryRequest): SetEntryReply {
        logger.debug("Received set entry request [{}]", request)
        return try {
            val input = SetEntry.Input(ByteBuffer.wrap(request.entry.toByteArray()))
            val output = node.setEntry(input)
            createSuccessSetEntryReply(output.leaderId)
        } catch (e: NotLeaderException) {
            logger.warn("Received set entry request while not leader=[{}]", e.knownLeaderId, e)
            createFailureSetEntryReply(e.knownLeaderId, SetEntryReply.Status.NOT_LEADER)
        } catch (e: CommitIndexMismatchException) {
            logger.warn("Commit index mismatch for set entry request", e)
            createFailureSetEntryReply(e.leaderId, SetEntryReply.Status.COMMIT_ERROR)
        } catch (e: LockNotAcquiredException) {
            logger.warn("Unable to complete request since node is busy", e)
            createFailureSetEntryReply(null, SetEntryReply.Status.PROCESSING)
        } catch (e: Exception) {
            logger.error("Unable to complete request due unknown error", e)
            createFailureSetEntryReply(null, SetEntryReply.Status.UNKNOWN)
        }
    }

    override suspend fun getEntry(request: GetEntryRequest): GetEntryReply {
        logger.debug("Received get entry request [{}]", request)
        return try {
            val input = GetEntry.Input(request.key)
            val output = node.getEntry(input)
            createSuccessGetEntryReply(output.leaderId, output.entry)
        } catch (e: NotLeaderException) {
            logger.warn("Received get entry request while not leader=[{}]", e.knownLeaderId, e)
            createFailureGetEntryReply(e.knownLeaderId, GetEntryReply.Status.NOT_LEADER)
        } catch (e: LockNotAcquiredException) {
            logger.warn("Unable to complete request since node is busy", e)
            createFailureGetEntryReply(null, GetEntryReply.Status.PROCESSING)
        } catch (e: Exception) {
            logger.error("Unable to complete request due unknown error", e)
            createFailureGetEntryReply(null, GetEntryReply.Status.UNKNOWN)
        }
    }

    private fun createSuccessSetEntryReply(leaderId: String?): SetEntryReply {
        return SetEntryReply.newBuilder().apply {
            this.timestamp = clock.millis()
            leaderId?.let { this.leaderId = it }
            this.status = SetEntryReply.Status.SUCCESS
        }.build()
    }

    private fun createFailureSetEntryReply(leaderId: String?, status: SetEntryReply.Status): SetEntryReply {
        return SetEntryReply.newBuilder().apply {
            this.timestamp = clock.millis()
            leaderId?.let { this.leaderId = it }
            this.status = status
        }.build()
    }

    private fun createSuccessGetEntryReply(leaderId: String?, data: ByteBuffer?): GetEntryReply {
        return GetEntryReply.newBuilder().apply {
            this.timestamp = clock.millis()
            leaderId?.let { this.leaderId = it }
            this.entry = data?.let { ByteString.copyFrom(it) }
            this.status = GetEntryReply.Status.SUCCESS
        }.build()
    }

    private fun createFailureGetEntryReply(leaderId: String?, status: GetEntryReply.Status): GetEntryReply {
        return GetEntryReply.newBuilder().apply {
            this.timestamp = clock.millis()
            leaderId?.let { this.leaderId = it }
            this.status = status
        }.build()
    }
}
