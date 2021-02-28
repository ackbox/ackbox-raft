package com.ackbox.raft.types

import com.ackbox.random.krandom
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.UUID

internal class MetadataTest {

    @Test
    fun `should deny new leader if already voted for a candidate`() {
        val votedCandidateId = krandom<String>()
        val anotherCandidateId = krandom<String>()
        val metadata = Metadata(UUID.randomUUID().toString(), Partition.GLOBAL)

        metadata.updateVote(votedCandidateId)

        assertFalse(metadata.canAcceptLeader(anotherCandidateId))
    }

    @Test
    fun `should accept new leader if already voted for the same candidate`() {
        val votedCandidateId = krandom<String>()
        val metadata = Metadata(UUID.randomUUID().toString(), Partition.GLOBAL)

        metadata.updateVote(votedCandidateId)

        assertTrue(metadata.canAcceptLeader(votedCandidateId))
    }

    @Test
    fun `should accept new leader if not voted yet`() {
        val votedCandidateId = krandom<String>()
        val metadata = Metadata(UUID.randomUUID().toString(), Partition.GLOBAL)

        assertTrue(metadata.canAcceptLeader(votedCandidateId))
    }

    @Test
    fun `should match leaderId if no leader has been set yet`() {
        val leaderId = krandom<String>()
        val metadata = Metadata(UUID.randomUUID().toString(), Partition.GLOBAL)

        assertTrue(metadata.matchesLeaderId(leaderId))
    }

    @Test
    fun `should match leaderId if proposed leader matches leaderId`() {
        val leaderId = krandom<String>()
        val metadata = Metadata(UUID.randomUUID().toString(), Partition.GLOBAL)

        metadata.updateLeaderId(leaderId)

        assertTrue(metadata.matchesLeaderId(leaderId))
    }

    @Test
    fun `should not match leaderId if proposed leader does not match leaderId`() {
        val leaderId = krandom<String>()
        val anotherLeaderId = krandom<String>()
        val metadata = Metadata(UUID.randomUUID().toString(), Partition.GLOBAL)

        metadata.updateLeaderId(leaderId)

        assertFalse(metadata.matchesLeaderId(anotherLeaderId))
    }

    @Test
    fun `should reset votedFor when updating loaderId`() {
        val candidateId = krandom<String>()
        val leaderId = krandom<String>()
        val metadata = Metadata(UUID.randomUUID().toString(), Partition.GLOBAL)

        metadata.updateVote(candidateId)
        metadata.updateLeaderId(leaderId)

        assertTrue(metadata.canAcceptLeader(leaderId))
        assertTrue(metadata.canAcceptLeader(candidateId))
    }

    @Test
    fun `should update commit index`() {
        val metadata = Metadata(UUID.randomUUID().toString(), Partition.GLOBAL)
        val lastAppliedIndex = krandom<Index>()
        val lastAppliedTerm = krandom<Term>()
        val newCommitIndex = krandom<Index>()

        metadata.updateCommitMetadata(lastAppliedIndex, lastAppliedTerm, newCommitIndex)

        assertEquals(lastAppliedIndex, metadata.commitMetadata.lastAppliedLogIndex)
        assertEquals(lastAppliedTerm, metadata.commitMetadata.lastAppliedLogTerm)
        assertEquals(newCommitIndex, metadata.commitMetadata.commitIndex)
    }

    @Test
    fun `should not update term when updating as follower if operationTerm is less than currentTerm`() {
        val metadata = Metadata(UUID.randomUUID().toString(), Partition.GLOBAL)
        val operationTerm = krandom<Term>()

        metadata.updateAsFollower(operationTerm)
        metadata.updateAsCandidate()
        metadata.updateAsFollower(operationTerm)

        assertEquals(operationTerm.incremented(), metadata.consensusMetadata.currentTerm)
        assertEquals(NodeMode.FOLLOWER, metadata.consensusMetadata.mode)
    }

    @Test
    fun `should update term when updating as follower if operationTerm is greater than currentTerm`() {
        val metadata = Metadata(UUID.randomUUID().toString(), Partition.GLOBAL)
        val operationTerm = krandom<Term>()
        val nextOperationTerm = operationTerm.incremented()

        metadata.updateAsFollower(operationTerm)
        metadata.updateAsFollower(nextOperationTerm)

        assertEquals(nextOperationTerm, metadata.consensusMetadata.currentTerm)
        assertEquals(NodeMode.FOLLOWER, metadata.consensusMetadata.mode)
    }

    @Test
    fun `should increment term when updating as candidate`() {
        val metadata = Metadata(UUID.randomUUID().toString(), Partition.GLOBAL)
        val operationTerm = krandom<Term>()

        metadata.updateAsFollower(operationTerm)
        metadata.updateAsCandidate()

        assertEquals(operationTerm.incremented(), metadata.consensusMetadata.currentTerm)
        assertEquals(NodeMode.CANDIDATE, metadata.consensusMetadata.mode)
    }

    @Test
    fun `should vote for itself term when updating as candidate`() {
        val nodeId = krandom<String>()
        val metadata = Metadata(nodeId, Partition.GLOBAL)

        metadata.updateAsCandidate()

        assertTrue(metadata.canAcceptLeader(nodeId))
        assertTrue(metadata.matchesLeaderId(nodeId))
        assertNull(metadata.consensusMetadata.leaderId)
        assertEquals(NodeMode.CANDIDATE, metadata.consensusMetadata.mode)
    }

    @Test
    fun `should vote for itself term when updating as leader`() {
        val nodeId = krandom<String>()
        val metadata = Metadata(nodeId, Partition.GLOBAL)

        metadata.updateAsLeader()

        assertTrue(metadata.canAcceptLeader(nodeId))
        assertTrue(metadata.matchesLeaderId(nodeId))
        assertEquals(nodeId, metadata.consensusMetadata.leaderId)
        assertEquals(NodeMode.LEADER, metadata.consensusMetadata.mode)
    }
}
