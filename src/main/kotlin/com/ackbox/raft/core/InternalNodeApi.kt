package com.ackbox.raft.core

import com.ackbox.raft.api.AppendReply
import com.ackbox.raft.api.AppendRequest
import com.ackbox.raft.api.PrivateNodeGrpcKt.PrivateNodeCoroutineImplBase
import com.ackbox.raft.api.SnapshotReply
import com.ackbox.raft.api.SnapshotRequest
import com.ackbox.raft.api.VoteReply
import com.ackbox.raft.api.VoteRequest
import com.ackbox.raft.log.LogItem
import com.ackbox.raft.state.Index
import com.ackbox.raft.state.Term
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
                Term(request.leaderTerm),
                Index(request.previousLogIndex),
                Term(request.previousLogTerm),
                Index(request.leaderCommitIndex),
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
            createFailureAppendReply(Term.UNDEFINED, Index.UNDEFINED, AppendReply.Status.PROCESSING)
        } catch (e: Exception) {
            logger.error("Unable to complete request due unknown error", e)
            val term = Term(request.leaderTerm)
            val lastLogIndex = Index(request.previousLogIndex)
            createFailureAppendReply(term, lastLogIndex, AppendReply.Status.UNKNOWN)
        }
    }

    override suspend fun handleVote(request: VoteRequest): VoteReply {
        logger.debug("Received vote request [{}]", request)
        return try {
            val input = ReplicaNode.Vote.Input(
                request.candidateId,
                Term(request.candidateTerm),
                Index(request.lastLogIndex),
                Term(request.lastLogTerm)
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
            createFailureVoteReply(Term.UNDEFINED, VoteReply.Status.PROCESSING)
        } catch (e: Exception) {
            logger.error("Unable to complete request due unknown error", e)
            createFailureVoteReply(Term(request.candidateTerm), VoteReply.Status.UNKNOWN)
        }
    }

    override suspend fun handleSnapshot(requests: Flow<SnapshotRequest>): SnapshotReply {
        logger.debug("Received snapshot streaming request")
        // If [processingSnapshot] cannot be atomically set, it means that the node is still
        // processing a previous snapshot request.
        if (!processingSnapshot.compareAndSet(false, true)) {
            return createFailureSnapshotReply(Term.UNDEFINED, SnapshotReply.Status.PROCESSING)
        }
        return try {
            val inputs = requests.flowOn(Dispatchers.IO).transform { request ->
                emit(
                    ReplicaNode.Snapshot.Input(
                        request.leaderId,
                        Term(request.leaderTerm),
                        Index(request.lastIncludedLogIndex),
                        Term(request.lastIncludedLogTerm),
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
            createFailureSnapshotReply(Term.UNDEFINED, SnapshotReply.Status.UNKNOWN)
        } finally {
            processingSnapshot.set(false)
        }
    }

    private fun createSuccessAppendReply(term: Term, lastLogIndex: Index): AppendReply {
        return AppendReply.newBuilder().apply {
            this.timestamp = clock.millis()
            this.currentTerm = term.value
            this.lastLogIndex = lastLogIndex.value
            this.status = AppendReply.Status.SUCCESS
        }.build()
    }

    private fun createFailureAppendReply(term: Term, lastLogIndex: Index, status: AppendReply.Status): AppendReply {
        return AppendReply.newBuilder().apply {
            this.timestamp = clock.millis()
            this.currentTerm = term.value
            this.lastLogIndex = lastLogIndex.value
            this.status = status
        }.build()
    }

    private fun createSuccessVoteReply(term: Term): VoteReply {
        return VoteReply.newBuilder().apply {
            this.timestamp = clock.millis()
            this.currentTerm = term.value
            this.status = VoteReply.Status.VOTE_GRANTED
        }.build()
    }

    private fun createFailureVoteReply(term: Term, status: VoteReply.Status): VoteReply {
        return VoteReply.newBuilder().apply {
            this.timestamp = clock.millis()
            this.currentTerm = term.value
            this.status = status
        }.build()
    }

    private fun createSuccessSnapshotReply(term: Term): SnapshotReply {
        return SnapshotReply.newBuilder().apply {
            this.timestamp = clock.millis()
            this.currentTerm = term.value
            this.status = SnapshotReply.Status.SUCCESS
        }.build()
    }

    private fun createFailureSnapshotReply(term: Term, status: SnapshotReply.Status): SnapshotReply {
        return SnapshotReply.newBuilder().apply {
            this.timestamp = clock.millis()
            this.currentTerm = term.value
            this.status = status
        }.build()
    }

    private fun AppendRequest.Entry.toLogItem(): LogItem {
        return LogItem(Index(index), Term(term), entry.toByteArray())
    }
}
