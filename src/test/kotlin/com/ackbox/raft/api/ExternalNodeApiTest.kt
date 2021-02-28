package com.ackbox.raft.api

import com.ackbox.raft.core.LeaderNode
import com.ackbox.raft.support.CommitIndexMismatchException
import com.ackbox.raft.support.LockNotAcquiredException
import com.ackbox.raft.support.NotLeaderException
import com.ackbox.raft.types.Index
import com.ackbox.random.krandom
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer
import java.time.Clock

internal class ExternalNodeApiTest {

    private val node = mock<LeaderNode> { on { nodeId } doReturn krandom() }

    @Test
    fun `should success set request`() {
        val api = ExternalNodeApi(node, Clock.systemUTC())
        val output = LeaderNode.SetEntry.Output(LEADER_ID)

        whenever(node.setEntry(any())).thenReturn(output)

        val reply = runBlocking { api.setEntry(SetEntryRequest.getDefaultInstance()) }

        assertEquals(output.leaderId, reply.leaderId)
        assertEquals(SetEntryReply.Status.SUCCESS, reply.status)
    }

    @Test
    fun `should fail set request in case of NotLeaderException`() {
        val api = ExternalNodeApi(node, Clock.systemUTC())

        whenever(node.setEntry(any())).thenThrow(NotLeaderException(LEADER_ID))

        val reply = runBlocking { api.setEntry(SetEntryRequest.getDefaultInstance()) }

        assertEquals(LEADER_ID, reply.leaderId)
        assertEquals(SetEntryReply.Status.NOT_LEADER, reply.status)
    }

    @Test
    fun `should fail set request in case of CommitIndexMismatchException`() {
        val api = ExternalNodeApi(node, Clock.systemUTC())

        whenever(node.setEntry(any())).thenThrow(CommitIndexMismatchException(LEADER_ID, COMMIT_INDEX, COMMIT_INDEX))

        val reply = runBlocking { api.setEntry(SetEntryRequest.getDefaultInstance()) }

        assertEquals(LEADER_ID, reply.leaderId)
        assertEquals(SetEntryReply.Status.COMMIT_ERROR, reply.status)
    }

    @Test
    fun `should fail set request in case of LockNotAcquiredException`() {
        val api = ExternalNodeApi(node, Clock.systemUTC())

        whenever(node.setEntry(any())).thenThrow(LockNotAcquiredException())

        val reply = runBlocking { api.setEntry(SetEntryRequest.getDefaultInstance()) }

        assertTrue(reply.leaderId.isBlank())
        assertEquals(SetEntryReply.Status.PROCESSING, reply.status)
    }

    @Test
    fun `should fail set request in case of Exception`() {
        val api = ExternalNodeApi(node, Clock.systemUTC())

        whenever(node.setEntry(any())).thenThrow(RuntimeException())

        val reply = runBlocking { api.setEntry(SetEntryRequest.getDefaultInstance()) }

        assertTrue(reply.leaderId.isBlank())
        assertEquals(SetEntryReply.Status.UNKNOWN, reply.status)
    }

    @Test
    fun `should success get request`() {
        val api = ExternalNodeApi(node, Clock.systemUTC())
        val output = LeaderNode.GetEntry.Output(LEADER_ID, DATA)

        whenever(node.getEntry(any())).thenReturn(output)

        val reply = runBlocking { api.getEntry(GetEntryRequest.getDefaultInstance()) }

        assertEquals(output.leaderId, reply.leaderId)
        assertArrayEquals(output.entry?.array(), reply.entry.toByteArray())
        assertEquals(GetEntryReply.Status.SUCCESS, reply.status)
    }

    @Test
    fun `should fail get request in case of NotLeaderException`() {
        val api = ExternalNodeApi(node, Clock.systemUTC())

        whenever(node.getEntry(any())).thenThrow(NotLeaderException(LEADER_ID))

        val reply = runBlocking { api.getEntry(GetEntryRequest.getDefaultInstance()) }

        assertEquals(LEADER_ID, reply.leaderId)
        assertArrayEquals(ByteArray(0), reply.entry.toByteArray())
        assertEquals(GetEntryReply.Status.NOT_LEADER, reply.status)
    }

    @Test
    fun `should fail get request in case of LockNotAcquiredException`() {
        val api = ExternalNodeApi(node, Clock.systemUTC())

        whenever(node.getEntry(any())).thenThrow(LockNotAcquiredException())

        val reply = runBlocking { api.getEntry(GetEntryRequest.getDefaultInstance()) }

        assertTrue(reply.leaderId.isBlank())
        assertArrayEquals(ByteArray(0), reply.entry.toByteArray())
        assertEquals(GetEntryReply.Status.PROCESSING, reply.status)
    }

    @Test
    fun `should fail get request in case of Exception`() {
        val api = ExternalNodeApi(node, Clock.systemUTC())

        whenever(node.getEntry(any())).thenThrow(RuntimeException())

        val reply = runBlocking { api.getEntry(GetEntryRequest.getDefaultInstance()) }

        assertTrue(reply.leaderId.isBlank())
        assertArrayEquals(ByteArray(0), reply.entry.toByteArray())
        assertEquals(GetEntryReply.Status.UNKNOWN, reply.status)
    }

    companion object {

        private val LEADER_ID = krandom<String>()
        private val COMMIT_INDEX = krandom<Index>()
        private val DATA = krandom<ByteBuffer>()
    }
}
