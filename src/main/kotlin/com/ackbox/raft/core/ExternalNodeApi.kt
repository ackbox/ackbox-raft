package com.ackbox.raft.core

import com.ackbox.raft.GetReply
import com.ackbox.raft.GetRequest
import com.ackbox.raft.PublicNodeGrpcKt.PublicNodeCoroutineImplBase
import com.ackbox.raft.SetReply
import com.ackbox.raft.SetRequest
import com.ackbox.raft.core.LeaderNode.Get
import com.ackbox.raft.core.LeaderNode.Set
import com.ackbox.raft.state.ReplicatedLog
import com.ackbox.raft.support.CommitIndexMismatchException
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

    private val logger = NodeLogger.from(node.nodeId, ExternalNodeApi::class)

    override suspend fun set(request: SetRequest): SetReply {
        logger.debug("Received SET request [{}]", request)
        return try {
            val input = Set.Input(listOf(ByteBuffer.wrap(request.entry.toByteArray())))
            val output = node.setItem(input)
            createSuccessSetReply(output.leaderId, output.itemSqn)
        } catch (e: NotLeaderException) {
            logger.warn("Received SET request while not leader", e.knownLeaderId, e)
            createFailureSetReply(e.knownLeaderId, SetReply.Status.NOT_LEADER)
        } catch (e: CommitIndexMismatchException) {
            logger.warn("Commit index mismatch for SET request", e)
            createFailureSetReply(e.leaderId, SetReply.Status.COMMIT_ERROR)
        }
    }

    override suspend fun get(request: GetRequest): GetReply {
        logger.debug("Received GET request [{}]", request)
        return try {
            val input = Get.Input(request.sqn)
            val output = node.getItem(input)
            createSuccessGetReply(output.leaderId, output.item)
        } catch (e: NotLeaderException) {
            logger.warn("Received GET request while not leader", e.knownLeaderId, e)
            createFailureGetReply(e.knownLeaderId, GetReply.Status.NOT_LEADER)
        }
    }

    private fun createSuccessSetReply(leaderId: String?, sqn: Long): SetReply {
        return SetReply.newBuilder()
            .setTimestamp(clock.millis())
            .setSqn(sqn)
            .setLeaderId(leaderId)
            .setStatus(SetReply.Status.SUCCESS)
            .build()
    }

    private fun createFailureSetReply(leaderId: String?, status: SetReply.Status): SetReply {
        return SetReply.newBuilder()
            .setTimestamp(clock.millis())
            .setSqn(UNDEFINED_ID)
            .setLeaderId(leaderId)
            .setStatus(status)
            .build()
    }

    private fun createSuccessGetReply(leaderId: String?, item: ReplicatedLog.LogItem?): GetReply {
        return GetReply.newBuilder()
            .setTimestamp(clock.millis())
            .setLeaderId(leaderId)
            .setEntry(item?.let { ByteString.copyFrom(it.value) })
            .setStatus(GetReply.Status.SUCCESS)
            .build()
    }

    private fun createFailureGetReply(leaderId: String?, status: GetReply.Status): GetReply {
        return GetReply.newBuilder()
            .setTimestamp(clock.millis())
            .setLeaderId(leaderId)
            .setStatus(status)
            .build()
    }
}
