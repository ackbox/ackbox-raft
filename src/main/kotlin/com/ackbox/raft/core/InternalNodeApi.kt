package com.ackbox.raft.core

import com.ackbox.raft.api.AppendReply
import com.ackbox.raft.api.AppendRequest
import com.ackbox.raft.api.PrivateNodeGrpcKt.PrivateNodeCoroutineImplBase
import com.ackbox.raft.api.SnapshotReply
import com.ackbox.raft.api.SnapshotRequest
import com.ackbox.raft.api.VoteReply
import com.ackbox.raft.api.VoteRequest
import com.ackbox.raft.log.LogItem
import com.ackbox.raft.support.LeaderMismatchException
import com.ackbox.raft.support.LockNotAcquiredException
import com.ackbox.raft.support.NodeLogger
import com.ackbox.raft.support.ReplicaStateMismatchException
import com.ackbox.raft.support.RequestTermInvariantException
import com.ackbox.raft.support.VoteNotGrantedException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.transform
import java.nio.ByteBuffer
import java.time.Clock
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Internal Raft node API implementation. The behavior implemented here follows the original paper.
 */
class InternalNodeApi(private val node: ReplicaNode, private val clock: Clock) : PrivateNodeCoroutineImplBase() {

    private val logger: NodeLogger = NodeLogger.from(node.nodeId, InternalNodeApi::class)
    private val processingSnapshot: AtomicBoolean = AtomicBoolean(false)

    override suspend fun handleAppend(request: AppendRequest): AppendReply {
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
            createFailureAppendReply(e.term, e.lastLogIndex, AppendReply.Status.LEADER_MISMATCH)
        } catch (e: RequestTermInvariantException) {
            logger.warn("Unable to complete request due to termination invariant violation", e)
            createFailureAppendReply(e.term, e.lastLogIndex, AppendReply.Status.TERM_MISMATCH)
        } catch (e: ReplicaStateMismatchException) {
            logger.warn("Unable to complete request due log state mismatch", e)
            createFailureAppendReply(e.term, e.lastLogIndex, AppendReply.Status.LOG_STATE_MISMATCH)
        } catch (e: LockNotAcquiredException) {
            logger.warn("Unable to complete request since node is busy", e)
            createFailureAppendReply(UNDEFINED_ID, UNDEFINED_ID, AppendReply.Status.PROCESSING)
        } catch (e: Exception) {
            logger.error("Unable to complete request due unknown error", e)
            createFailureAppendReply(request.leaderTerm, request.previousLogIndex, AppendReply.Status.UNRECOGNIZED)
        }
    }

    override suspend fun handleVote(request: VoteRequest): VoteReply {
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
            createFailureVoteReply(e.term, VoteReply.Status.VOTE_NOT_GRANTED)
        } catch (e: VoteNotGrantedException) {
            logger.warn("Unable to vote for candidate", e)
            createFailureVoteReply(e.term, VoteReply.Status.VOTE_NOT_GRANTED)
        } catch (e: LockNotAcquiredException) {
            logger.warn("Unable to complete request since node is busy", e)
            createFailureVoteReply(UNDEFINED_ID, VoteReply.Status.PROCESSING)
        } catch (e: Exception) {
            logger.error("Unable to complete request due unknown error", e)
            createFailureVoteReply(request.candidateTerm, VoteReply.Status.VOTE_NOT_GRANTED)
        }
    }

    override suspend fun handleSnapshot(requests: Flow<SnapshotRequest>): SnapshotReply {
        logger.debug("Received snapshot streaming request")
        // If [processingSnapshot] cannot be atomically set, it means that the node is still
        // processing a previous snapshot request.
        if (!processingSnapshot.compareAndSet(false, true)) {
            return createFailureSnapshotReply(UNDEFINED_ID, SnapshotReply.Status.PROCESSING)
        }
        return try {
            val inputs = requests.flowOn(Dispatchers.IO).transform { request ->
                emit(
                    ReplicaNode.Snapshot.Input(
                        request.leaderId,
                        request.leaderTerm,
                        request.lastIncludedLogIndex,
                        request.lastIncludedLogTerm,
                        ByteBuffer.wrap(request.data.toByteArray())
                    )
                )
            }
            val output = node.handleSnapshot(inputs)
            createSuccessSnapshotReply(output.currentTerm)
        } catch (e: LeaderMismatchException) {
            logger.warn("Unable to complete request due to leader mismatch", e)
            createFailureSnapshotReply(e.term, SnapshotReply.Status.LEADER_MISMATCH)
        } catch (e: RequestTermInvariantException) {
            logger.warn("Unable to complete request due to termination invariant violation", e)
            createFailureSnapshotReply(e.term, SnapshotReply.Status.TERM_MISMATCH)
        } catch (e: Exception) {
            logger.error("Unable to complete request due unknown error", e)
            createFailureSnapshotReply(UNDEFINED_ID, SnapshotReply.Status.UNRECOGNIZED)
        } finally {
            processingSnapshot.set(false)
        }
    }

    private fun createSuccessAppendReply(term: Long, lastLogIndex: Long): AppendReply {
        return AppendReply.newBuilder()
            .setTimestamp(clock.millis())
            .setCurrentTerm(term)
            .setLastLogIndex(lastLogIndex)
            .setStatus(AppendReply.Status.SUCCESS)
            .build()
    }

    private fun createFailureAppendReply(term: Long, lastLogIndex: Long, status: AppendReply.Status): AppendReply {
        return AppendReply.newBuilder()
            .setTimestamp(clock.millis())
            .setCurrentTerm(term)
            .setLastLogIndex(lastLogIndex)
            .setStatus(status)
            .build()
    }

    private fun createSuccessVoteReply(term: Long): VoteReply {
        return VoteReply.newBuilder()
            .setTimestamp(clock.millis())
            .setCurrentTerm(term)
            .setStatus(VoteReply.Status.VOTE_GRANTED)
            .build()
    }

    private fun createFailureVoteReply(term: Long, status: VoteReply.Status): VoteReply {
        return VoteReply.newBuilder()
            .setTimestamp(clock.millis())
            .setCurrentTerm(term)
            .setStatus(status)
            .build()
    }

    private fun createSuccessSnapshotReply(term: Long): SnapshotReply {
        return SnapshotReply.newBuilder()
            .setTimestamp(clock.millis())
            .setCurrentTerm(term)
            .setStatus(SnapshotReply.Status.SUCCESS)
            .build()
    }

    private fun createFailureSnapshotReply(term: Long, status: SnapshotReply.Status): SnapshotReply {
        return SnapshotReply.newBuilder()
            .setTimestamp(clock.millis())
            .setCurrentTerm(term)
            .setStatus(status)
            .build()
    }

    private fun AppendRequest.Entry.toLogItem(): LogItem {
        return LogItem(index, term, entry.toByteArray())
    }
}
