package com.ackbox.raft.api

import com.ackbox.raft.core.LeaderNode
import com.ackbox.raft.support.LockNotAcquiredException
import com.ackbox.raft.support.NotLeaderException
import com.ackbox.random.krandom
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Clock

internal class ManagementNodeApiTest {

    private val node = mock<LeaderNode> { on { nodeId } doReturn krandom() }

    @Test
    fun `should success add node request`() {
        val api = ManagementNodeApi(node, Clock.systemUTC())
        val output = LeaderNode.AddNode.Output(LEADER_ID)

        whenever(node.addNode(any())).thenReturn(output)

        val reply = runBlocking { api.addNode(AddNodeRequest.getDefaultInstance()) }

        assertEquals(output.leaderId, reply.leaderId)
        assertEquals(AddNodeReply.Status.SUCCESS, reply.status)
    }

    @Test
    fun `should fail add node request in case of NotLeaderException`() {
        val api = ManagementNodeApi(node, Clock.systemUTC())

        whenever(node.addNode(any())).thenThrow(NotLeaderException(LEADER_ID))

        val reply = runBlocking { api.addNode(AddNodeRequest.getDefaultInstance()) }

        assertEquals(LEADER_ID, reply.leaderId)
        assertEquals(AddNodeReply.Status.NOT_LEADER, reply.status)
    }

    @Test
    fun `should fail add node request in case of LockNotAcquiredException`() {
        val api = ManagementNodeApi(node, Clock.systemUTC())

        whenever(node.addNode(any())).thenThrow(LockNotAcquiredException())

        val reply = runBlocking { api.addNode(AddNodeRequest.getDefaultInstance()) }

        assertTrue(reply.leaderId.isBlank())
        assertEquals(AddNodeReply.Status.PROCESSING, reply.status)
    }

    @Test
    fun `should fail add node request in case of Exception`() {
        val api = ManagementNodeApi(node, Clock.systemUTC())

        whenever(node.addNode(any())).thenThrow(RuntimeException())

        val reply = runBlocking { api.addNode(AddNodeRequest.getDefaultInstance()) }

        assertTrue(reply.leaderId.isBlank())
        assertEquals(AddNodeReply.Status.UNKNOWN, reply.status)
    }

    @Test
    fun `should success remove node request`() {
        val api = ManagementNodeApi(node, Clock.systemUTC())
        val output = LeaderNode.RemoveNode.Output(LEADER_ID)

        whenever(node.removeNode(any())).thenReturn(output)

        val reply = runBlocking { api.removeNode(RemoveNodeRequest.getDefaultInstance()) }

        assertEquals(output.leaderId, reply.leaderId)
        assertEquals(RemoveNodeReply.Status.SUCCESS, reply.status)
    }

    @Test
    fun `should fail remove node request in case of NotLeaderException`() {
        val api = ManagementNodeApi(node, Clock.systemUTC())

        whenever(node.removeNode(any())).thenThrow(NotLeaderException(LEADER_ID))

        val reply = runBlocking { api.removeNode(RemoveNodeRequest.getDefaultInstance()) }

        assertEquals(LEADER_ID, reply.leaderId)
        assertEquals(RemoveNodeReply.Status.NOT_LEADER, reply.status)
    }

    @Test
    fun `should fail remove node request in case of LockNotAcquiredException`() {
        val api = ManagementNodeApi(node, Clock.systemUTC())

        whenever(node.removeNode(any())).thenThrow(LockNotAcquiredException())

        val reply = runBlocking { api.removeNode(RemoveNodeRequest.getDefaultInstance()) }

        assertTrue(reply.leaderId.isBlank())
        assertEquals(RemoveNodeReply.Status.PROCESSING, reply.status)
    }

    @Test
    fun `should fail remove node request in case of Exception`() {
        val api = ManagementNodeApi(node, Clock.systemUTC())

        whenever(node.removeNode(any())).thenThrow(RuntimeException())

        val reply = runBlocking { api.removeNode(RemoveNodeRequest.getDefaultInstance()) }

        assertTrue(reply.leaderId.isBlank())
        assertEquals(RemoveNodeReply.Status.UNKNOWN, reply.status)
    }

    companion object {

        private val LEADER_ID = krandom<String>()
    }
}
