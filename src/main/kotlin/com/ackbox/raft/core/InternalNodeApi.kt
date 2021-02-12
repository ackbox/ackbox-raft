package com.ackbox.raft.core

import com.ackbox.raft.AppendReply
import com.ackbox.raft.AppendReply.Status
import com.ackbox.raft.AppendRequest
import com.ackbox.raft.PrivateNodeGrpcKt.PrivateNodeCoroutineImplBase
import com.ackbox.raft.VoteReply
import com.ackbox.raft.VoteRequest
import com.ackbox.raft.state.ReplicatedLog
import com.ackbox.raft.support.LeaderMismatchException
import com.ackbox.raft.support.NodeLogger
import com.ackbox.raft.support.ReplicaStateMismatchException
import com.ackbox.raft.support.RequestTermInvariantException
import com.ackbox.raft.support.VoteNotGrantedException
import java.nio.ByteBuffer
import java.time.Clock

/**
 * Internal Raft node API implementation. The behavior implemented here follows the original paper.
 */
class InternalNodeApi(private val node: ReplicaNode, private val clock: Clock) : PrivateNodeCoroutineImplBase() {

    private val logger = NodeLogger.from(node.nodeId, InternalNodeApi::class)

    override suspend fun append(request: AppendRequest): AppendReply {
        logger.debug("Received append request [{}]", request)
        return try {
            val input = ReplicaNode.Append.Input(
                request.leaderId,
                request.leaderTerm,
                request.previousLogIndex,
                request.previousLogTerm,
                request.leaderCommitIndex,
                request.entriesList.map { it.toLogItem() }
            )
            val output = node.handleAppend(input)
            createSuccessAppendReply(output.currentTerm, output.lastLogItemIndex)
        } catch (e: LeaderMismatchException) {
            logger.warn("Unable to complete request due to leader mismatch", e)
            createFailureAppendReply(e.term, e.lastLogIndex, Status.LEADER_MISMATCH)
        } catch (e: RequestTermInvariantException) {
            logger.warn("Unable to complete request due to termination invariant violation", e)
            createFailureAppendReply(e.term, e.lastLogIndex, Status.TERM_MISMATCH)
        } catch (e: ReplicaStateMismatchException) {
            logger.warn("Unable to complete request due log state mismatch", e)
            createFailureAppendReply(e.term, e.lastLogIndex, Status.LOG_STATE_MISMATCH)
        }
    }

    override suspend fun vote(request: VoteRequest): VoteReply {
        logger.debug("Received vote request [{}]", request)
        return try {
            val input = ReplicaNode.Vote.Input(
                request.candidateId,
                request.candidateTerm,
                request.lastLogIndex,
                request.lastLogTerm
            )
            val output = node.handleVote(input)
            createSuccessVoteReply(output.currentTerm)
        } catch (e: RequestTermInvariantException) {
            logger.warn("Unable to complete request due to termination invariant violation", e)
            createFailureVoteReply(e.term)
        } catch (e: VoteNotGrantedException) {
            logger.warn("Unable to vote for candidate", e)
            createFailureVoteReply(e.term)
        }
    }

    private fun createSuccessAppendReply(currentTerm: Long, lastLogIndex: Long): AppendReply {
        return AppendReply.newBuilder()
            .setTimestamp(clock.millis())
            .setCurrentTerm(currentTerm)
            .setLastLogIndex(lastLogIndex)
            .setStatus(Status.SUCCESS)
            .build()
    }

    private fun createFailureAppendReply(currentTerm: Long, lastLogIndex: Long, status: Status): AppendReply {
        return AppendReply.newBuilder()
            .setTimestamp(clock.millis())
            .setCurrentTerm(currentTerm)
            .setLastLogIndex(lastLogIndex)
            .setStatus(status)
            .build()
    }

    private fun createSuccessVoteReply(currentTerm: Long): VoteReply {
        return VoteReply.newBuilder()
            .setTimestamp(clock.millis())
            .setCurrentTerm(currentTerm)
            .setStatus(VoteReply.Status.VOTE_GRANTED)
            .build()
    }

    private fun createFailureVoteReply(currentTerm: Long): VoteReply {
        return VoteReply.newBuilder()
            .setTimestamp(clock.millis())
            .setCurrentTerm(currentTerm)
            .setStatus(VoteReply.Status.VOTE_NOT_GRANTED)
            .build()
    }

    private fun AppendRequest.Entry.toLogItem(): ReplicatedLog.LogItem {
        return ReplicatedLog.LogItem(index, term, ByteBuffer.wrap(toByteArray()))
    }
}
