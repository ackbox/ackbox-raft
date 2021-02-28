package com.ackbox.raft.api

import com.ackbox.raft.core.ReplicaNode
import com.ackbox.raft.support.LeaderMismatchException
import com.ackbox.raft.support.LockNotAcquiredException
import com.ackbox.raft.support.ReplicaStateMismatchException
import com.ackbox.raft.support.RequestTermInvariantException
import com.ackbox.raft.support.UnknownNodeException
import com.ackbox.raft.support.VoteNotGrantedException
import com.ackbox.raft.types.Index
import com.ackbox.raft.types.Term
import com.ackbox.random.krandom
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.doThrow
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.stub
import com.nhaarman.mockitokotlin2.whenever
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Clock

internal class InternalNodeApiTest {

    private val node = mock<ReplicaNode> { on { nodeId } doReturn krandom() }

    @Test
    fun `should success append request`() {
        val api = InternalNodeApi(node, Clock.systemUTC())
        val output = ReplicaNode.Append.Output(TERM, LAST_LOG_INDEX)

        whenever(node.handleAppend(any())).thenReturn(output)

        val reply = runBlocking { api.handleAppend(AppendRequest.getDefaultInstance()) }

        assertEquals(TERM.value, reply.currentTerm)
        assertEquals(LAST_LOG_INDEX.value, reply.lastLogIndex)
        assertEquals(AppendReply.Status.SUCCESS, reply.status)
    }

    @Test
    fun `should fail append request in case of LeaderMismatchException`() {
        val api = InternalNodeApi(node, Clock.systemUTC())
        val exception = LeaderMismatchException(LEADER_ID, TERM, LAST_LOG_INDEX)

        whenever(node.handleAppend(any())).thenThrow(exception)

        val reply = runBlocking { api.handleAppend(AppendRequest.getDefaultInstance()) }

        assertEquals(TERM.value, reply.currentTerm)
        assertEquals(LAST_LOG_INDEX.value, reply.lastLogIndex)
        assertEquals(AppendReply.Status.LEADER_MISMATCH, reply.status)
    }

    @Test
    fun `should fail append request in case of RequestTermInvariantException`() {
        val api = InternalNodeApi(node, Clock.systemUTC())
        val exception = RequestTermInvariantException(TERM, OTHER_TERM, LAST_LOG_INDEX)

        whenever(node.handleAppend(any())).thenThrow(exception)

        val reply = runBlocking { api.handleAppend(AppendRequest.getDefaultInstance()) }

        assertEquals(TERM.value, reply.currentTerm)
        assertEquals(LAST_LOG_INDEX.value, reply.lastLogIndex)
        assertEquals(AppendReply.Status.TERM_MISMATCH, reply.status)
    }

    @Test
    fun `should fail append request in case of ReplicaStateMismatchException`() {
        val api = InternalNodeApi(node, Clock.systemUTC())
        val exception = ReplicaStateMismatchException(TERM, LAST_LOG_INDEX)

        whenever(node.handleAppend(any())).thenThrow(exception)

        val reply = runBlocking { api.handleAppend(AppendRequest.getDefaultInstance()) }

        assertEquals(TERM.value, reply.currentTerm)
        assertEquals(LAST_LOG_INDEX.value, reply.lastLogIndex)
        assertEquals(AppendReply.Status.LOG_STATE_MISMATCH, reply.status)
    }

    @Test
    fun `should fail append request in case of LockNotAcquiredException`() {
        val api = InternalNodeApi(node, Clock.systemUTC())
        val exception = LockNotAcquiredException()

        whenever(node.handleAppend(any())).thenThrow(exception)

        val reply = runBlocking { api.handleAppend(AppendRequest.getDefaultInstance()) }

        assertEquals(Term.UNDEFINED.value, reply.currentTerm)
        assertEquals(Index.UNDEFINED.value, reply.lastLogIndex)
        assertEquals(AppendReply.Status.PROCESSING, reply.status)
    }

    @Test
    fun `should fail append request in case of UnknownNodeException`() {
        val api = InternalNodeApi(node, Clock.systemUTC())
        val exception = UnknownNodeException(LEADER_ID)

        whenever(node.handleAppend(any())).thenThrow(exception)

        val reply = runBlocking { api.handleAppend(AppendRequest.getDefaultInstance()) }

        assertEquals(Term.UNDEFINED.value, reply.currentTerm)
        assertEquals(Index.UNDEFINED.value, reply.lastLogIndex)
        assertEquals(AppendReply.Status.NODE_NOT_KNOWN, reply.status)
    }

    @Test
    fun `should fail append request in case of Exception`() {
        val api = InternalNodeApi(node, Clock.systemUTC())
        val exception = RuntimeException()
        val request = AppendRequest.getDefaultInstance()

        whenever(node.handleAppend(any())).thenThrow(exception)

        val reply = runBlocking { api.handleAppend(request) }

        assertEquals(request.leaderTerm, reply.currentTerm)
        assertEquals(request.previousLogIndex, reply.lastLogIndex)
        assertEquals(AppendReply.Status.UNKNOWN, reply.status)
    }

    @Test
    fun `should success vote request`() {
        val api = InternalNodeApi(node, Clock.systemUTC())
        val output = ReplicaNode.Vote.Output(TERM)

        whenever(node.handleVote(any())).thenReturn(output)

        val reply = runBlocking { api.handleVote(VoteRequest.getDefaultInstance()) }

        assertEquals(TERM.value, reply.currentTerm)
        assertEquals(VoteReply.Status.VOTE_GRANTED, reply.status)
    }

    @Test
    fun `should fail vote request in case of RequestTermInvariantException`() {
        val api = InternalNodeApi(node, Clock.systemUTC())
        val exception = RequestTermInvariantException(TERM, OTHER_TERM, LAST_LOG_INDEX)

        whenever(node.handleVote(any())).thenThrow(exception)

        val reply = runBlocking { api.handleVote(VoteRequest.getDefaultInstance()) }

        assertEquals(TERM.value, reply.currentTerm)
        assertEquals(VoteReply.Status.VOTE_NOT_GRANTED, reply.status)
    }

    @Test
    fun `should fail vote request in case of VoteNotGrantedException`() {
        val api = InternalNodeApi(node, Clock.systemUTC())
        val exception = VoteNotGrantedException(CANDIDATE_ID, OTHER_TERM)

        whenever(node.handleVote(any())).thenThrow(exception)

        val reply = runBlocking { api.handleVote(VoteRequest.getDefaultInstance()) }

        assertEquals(OTHER_TERM.value, reply.currentTerm)
        assertEquals(VoteReply.Status.VOTE_NOT_GRANTED, reply.status)
    }

    @Test
    fun `should fail vote request in case of LockNotAcquiredException`() {
        val api = InternalNodeApi(node, Clock.systemUTC())
        val exception = LockNotAcquiredException()

        whenever(node.handleVote(any())).thenThrow(exception)

        val reply = runBlocking { api.handleVote(VoteRequest.getDefaultInstance()) }

        assertEquals(Term.UNDEFINED.value, reply.currentTerm)
        assertEquals(VoteReply.Status.PROCESSING, reply.status)
    }

    @Test
    fun `should fail vote request in case of UnknownNodeException`() {
        val api = InternalNodeApi(node, Clock.systemUTC())
        val exception = UnknownNodeException(LEADER_ID)
        val request = VoteRequest.getDefaultInstance()

        whenever(node.handleVote(any())).thenThrow(exception)

        val reply = runBlocking { api.handleVote(request) }

        assertEquals(request.candidateTerm, reply.currentTerm)
        assertEquals(VoteReply.Status.NODE_NOT_KNOWN, reply.status)
    }

    @Test
    fun `should fail vote request in case of Exception`() {
        val api = InternalNodeApi(node, Clock.systemUTC())
        val exception = RuntimeException()
        val request = VoteRequest.getDefaultInstance()

        whenever(node.handleVote(any())).thenThrow(exception)

        val reply = runBlocking { api.handleVote(request) }

        assertEquals(request.candidateTerm, reply.currentTerm)
        assertEquals(VoteReply.Status.UNKNOWN, reply.status)
    }

    @Test
    fun `should success snapshot request`() {
        val api = InternalNodeApi(node, Clock.systemUTC())
        val output = ReplicaNode.Snapshot.Output(TERM)

        node.stub { onBlocking { handleSnapshot(any()) } doReturn output }

        val reply = runBlocking { api.handleSnapshot(flow { emit(SnapshotRequest.getDefaultInstance()) }) }

        assertEquals(TERM.value, reply.currentTerm)
        assertEquals(SnapshotReply.Status.SUCCESS, reply.status)
    }

    @Test
    fun `should fail snapshot request in case of LeaderMismatchException`() {
        val api = InternalNodeApi(node, Clock.systemUTC())
        val exception = LeaderMismatchException(LEADER_ID, OTHER_TERM, LAST_LOG_INDEX)

        node.stub { onBlocking { handleSnapshot(any()) } doThrow exception }

        val reply = runBlocking { api.handleSnapshot(flow { emit(SnapshotRequest.getDefaultInstance()) }) }

        assertEquals(OTHER_TERM.value, reply.currentTerm)
        assertEquals(SnapshotReply.Status.LEADER_MISMATCH, reply.status)
    }

    @Test
    fun `should fail snapshot request in case of RequestTermInvariantException`() {
        val api = InternalNodeApi(node, Clock.systemUTC())
        val exception = RequestTermInvariantException(TERM, OTHER_TERM, LAST_LOG_INDEX)

        node.stub { onBlocking { handleSnapshot(any()) } doThrow exception }

        val reply = runBlocking { api.handleSnapshot(flow { emit(SnapshotRequest.getDefaultInstance()) }) }

        assertEquals(TERM.value, reply.currentTerm)
        assertEquals(SnapshotReply.Status.TERM_MISMATCH, reply.status)
    }

    @Test
    fun `should fail snapshot request in case of UnknownNodeException`() {
        val api = InternalNodeApi(node, Clock.systemUTC())
        val exception = UnknownNodeException(LEADER_ID)

        node.stub { onBlocking { handleSnapshot(any()) } doThrow exception }

        val reply = runBlocking { api.handleSnapshot(flow { emit(SnapshotRequest.getDefaultInstance()) }) }

        assertEquals(Term.UNDEFINED.value, reply.currentTerm)
        assertEquals(SnapshotReply.Status.NODE_NOT_KNOWN, reply.status)
    }

    @Test
    fun `should fail snapshot request in case of Exception`() {
        val api = InternalNodeApi(node, Clock.systemUTC())
        val exception = RuntimeException()

        node.stub { onBlocking { handleSnapshot(any()) } doThrow exception }

        val reply = runBlocking { api.handleSnapshot(flow { emit(SnapshotRequest.getDefaultInstance()) }) }

        assertEquals(Term.UNDEFINED.value, reply.currentTerm)
        assertEquals(SnapshotReply.Status.UNKNOWN, reply.status)
    }

    companion object {

        private val LEADER_ID = krandom<String>()
        private val CANDIDATE_ID = krandom<String>()
        private val LAST_LOG_INDEX = krandom<Index>()
        private val TERM = krandom<Term>()
        private val OTHER_TERM = krandom<Term>()
    }
}
