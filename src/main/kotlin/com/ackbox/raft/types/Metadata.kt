package com.ackbox.raft.types

import com.ackbox.raft.support.NodeLogger
import javax.annotation.concurrent.NotThreadSafe

/**
 * Class defining basic metadata of the algorithm.
 * @param nodeId Unique identifier for the current node.
 * @param partition State partition that this metadata instance belongs to.
 */
@NotThreadSafe
class Metadata(val nodeId: String, val partition: Partition) {

    var consensusMetadata: ConsensusMetadata = ConsensusMetadata()
        private set

    var commitMetadata: CommitMetadata = CommitMetadata()
        private set

    private val logger = NodeLogger.forPartition(nodeId, partition, Metadata::class)

    fun canAcceptLeader(candidateId: String): Boolean {
        return consensusMetadata.votedFor == null || consensusMetadata.votedFor == candidateId
    }

    fun matchesLeaderId(proposedLeaderId: String): Boolean {
        return consensusMetadata.leaderId == null || consensusMetadata.leaderId == proposedLeaderId
    }

    fun updateVote(candidateId: String) {
        logger.info("Updating vote for candidate [{}]", candidateId)
        consensusMetadata = consensusMetadata.copy(votedFor = candidateId)
    }

    fun updateLeaderId(proposedLeaderId: String) {
        logger.info("Updating leader to leaderId [{}]", proposedLeaderId)
        consensusMetadata = consensusMetadata.copy(votedFor = null, leaderId = proposedLeaderId)
    }

    fun updateCommitMetadata(appliedLogIndex: Index, appliedLogTerm: Term, commitIndex: Index) {
        logger.info(
            "Updating commit metadata with logIndex=[{}], logTerm=[{}] and commitIndex=[{}]",
            appliedLogIndex,
            appliedLogTerm,
            commitIndex
        )
        commitMetadata = commitMetadata.copy(
            lastAppliedLogIndex = appliedLogIndex,
            lastAppliedLogTerm = appliedLogTerm,
            commitIndex = commitIndex
        )
    }

    fun updateAsFollower(operationTerm: Term) {
        logger.info("Updating node state as follower with term [{}]", operationTerm)
        val currentTerm = if (consensusMetadata.currentTerm < operationTerm) {
            logger.info("Updating term with term [{}] and resetting votedFor", operationTerm)
            operationTerm
        } else {
            consensusMetadata.currentTerm
        }
        consensusMetadata = consensusMetadata.copy(votedFor = null, currentTerm = currentTerm, mode = NodeMode.FOLLOWER)
    }

    fun updateAsCandidate() {
        logger.info("Updating node state as candidate")
        consensusMetadata = consensusMetadata.copy(
            currentTerm = consensusMetadata.currentTerm.incremented(),
            leaderId = null,
            votedFor = nodeId,
            mode = NodeMode.CANDIDATE
        )
    }

    fun updateAsLeader() {
        logger.info("Updating node state as leader")
        consensusMetadata = consensusMetadata.copy(
            leaderId = nodeId,
            votedFor = nodeId,
            mode = NodeMode.LEADER
        )
    }
}
