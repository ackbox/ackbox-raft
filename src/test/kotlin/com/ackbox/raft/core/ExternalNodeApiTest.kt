package com.ackbox.raft.core

import com.ackbox.raft.Fixtures
import com.ackbox.raft.api.GetReply
import com.ackbox.raft.api.GetRequest
import com.ackbox.raft.api.SetReply
import com.ackbox.raft.api.SetRequest
import com.ackbox.raft.support.CommitIndexMismatchException
import com.ackbox.raft.support.LockNotAcquiredException
import com.ackbox.raft.support.NotLeaderException
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
import java.time.Clock

internal class ExternalNodeApiTest {

    private val node = mock<LeaderNode> { on { nodeId } doReturn krandom() }

    @Test
    fun `should success set request`() {
        val api = ExternalNodeApi(node, Clock.systemUTC())
        val output = LeaderNode.Set.Output(LEADER_ID, SQN)

        whenever(node.setItem(any())).thenReturn(output)

        val reply = runBlocking { api.set(SetRequest.getDefaultInstance()) }

        assertEquals(output.leaderId, reply.leaderId)
        assertEquals(output.itemSqn, reply.sqn)
        assertEquals(SetReply.Status.SUCCESS, reply.status)
    }

    @Test
    fun `should fail set request in case of NotLeaderException`() {
        val api = ExternalNodeApi(node, Clock.systemUTC())

        whenever(node.setItem(any())).thenThrow(NotLeaderException(LEADER_ID))

        val reply = runBlocking { api.set(SetRequest.getDefaultInstance()) }

        assertEquals(LEADER_ID, reply.leaderId)
        assertEquals(UNDEFINED_ID, reply.sqn)
        assertEquals(SetReply.Status.NOT_LEADER, reply.status)
    }

    @Test
    fun `should fail set request in case of CommitIndexMismatchException`() {
        val api = ExternalNodeApi(node, Clock.systemUTC())

        whenever(node.setItem(any())).thenThrow(CommitIndexMismatchException(LEADER_ID, COMMIT_INDEX, COMMIT_INDEX))

        val reply = runBlocking { api.set(SetRequest.getDefaultInstance()) }

        assertEquals(LEADER_ID, reply.leaderId)
        assertEquals(UNDEFINED_ID, reply.sqn)
        assertEquals(SetReply.Status.COMMIT_ERROR, reply.status)
    }

    @Test
    fun `should fail set request in case of LockNotAcquiredException`() {
        val api = ExternalNodeApi(node, Clock.systemUTC())

        whenever(node.setItem(any())).thenThrow(LockNotAcquiredException())

        val reply = runBlocking { api.set(SetRequest.getDefaultInstance()) }

        assertTrue(reply.leaderId.isBlank())
        assertEquals(UNDEFINED_ID, reply.sqn)
        assertEquals(SetReply.Status.PROCESSING, reply.status)
    }

    @Test
    fun `should fail set request in case of Exception`() {
        val api = ExternalNodeApi(node, Clock.systemUTC())

        whenever(node.setItem(any())).thenThrow(RuntimeException())

        val reply = runBlocking { api.set(SetRequest.getDefaultInstance()) }

        assertTrue(reply.leaderId.isBlank())
        assertEquals(UNDEFINED_ID, reply.sqn)
        assertEquals(SetReply.Status.UNKNOWN, reply.status)
    }

    @Test
    fun `should success get request`() {
        val api = ExternalNodeApi(node, Clock.systemUTC())
        val output = LeaderNode.Get.Output(LEADER_ID, Fixtures.createLogItem(ITEM_INDEX))

        whenever(node.getItem(any())).thenReturn(output)

        val reply = runBlocking { api.get(GetRequest.getDefaultInstance()) }

        assertEquals(output.leaderId, reply.leaderId)
        assertArrayEquals(output.item?.value, reply.entry.toByteArray())
        assertEquals(GetReply.Status.SUCCESS, reply.status)
    }

    @Test
    fun `should fail get request in case of NotLeaderException`() {
        val api = ExternalNodeApi(node, Clock.systemUTC())

        whenever(node.getItem(any())).thenThrow(NotLeaderException(LEADER_ID))

        val reply = runBlocking { api.get(GetRequest.getDefaultInstance()) }

        assertEquals(LEADER_ID, reply.leaderId)
        assertArrayEquals(ByteArray(0), reply.entry.toByteArray())
        assertEquals(GetReply.Status.NOT_LEADER, reply.status)
    }

    @Test
    fun `should fail get request in case of LockNotAcquiredException`() {
        val api = ExternalNodeApi(node, Clock.systemUTC())

        whenever(node.getItem(any())).thenThrow(LockNotAcquiredException())

        val reply = runBlocking { api.get(GetRequest.getDefaultInstance()) }

        assertTrue(reply.leaderId.isBlank())
        assertArrayEquals(ByteArray(0), reply.entry.toByteArray())
        assertEquals(GetReply.Status.PROCESSING, reply.status)
    }

    @Test
    fun `should fail get request in case of Exception`() {
        val api = ExternalNodeApi(node, Clock.systemUTC())

        whenever(node.getItem(any())).thenThrow(RuntimeException())

        val reply = runBlocking { api.get(GetRequest.getDefaultInstance()) }

        assertTrue(reply.leaderId.isBlank())
        assertArrayEquals(ByteArray(0), reply.entry.toByteArray())
        assertEquals(GetReply.Status.UNKNOWN, reply.status)
    }

    companion object {

        private val LEADER_ID = krandom<String>()
        private val ITEM_INDEX = krandom<Long>()
        private val COMMIT_INDEX = krandom<Long>()
        private val SQN = krandom<Long>()
    }
}
