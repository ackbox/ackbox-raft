package com.ackbox.raft.state

import com.ackbox.raft.networking.NodeNetworking
import com.ackbox.raft.support.NodeLogger
import javax.annotation.concurrent.NotThreadSafe

/**
 * Class defining basic metadata of the algorithm.
 * @param nodeId Unique identifier for the current node.
 */
@NotThreadSafe
class Metadata(val nodeId: String) {

    var consensusMetadata: ConsensusMetadata = ConsensusMetadata()
        private set

    var commitMetadata: CommitMetadata = CommitMetadata()
        private set

    private val logger = NodeLogger.from(nodeId, NodeNetworking::class)

    fun canAcceptLeader(candidateId: String): Boolean {
        return consensusMetadata.votedFor == null || consensusMetadata.votedFor == candidateId
    }

    fun matchesLeaderId(proposedLeaderId: String): Boolean {
        return consensusMetadata.leaderId == null || consensusMetadata.leaderId == proposedLeaderId
    }

    fun updateVote(candidateId: String) {
        logger.info("Updating vote for candidate=[{}]", candidateId)
        consensusMetadata = consensusMetadata.copy(votedFor = candidateId)
    }

    fun updateLeaderId(proposedLeaderId: String) {
        logger.info("Updating leader to leaderId=[{}]", proposedLeaderId)
        consensusMetadata = consensusMetadata.copy(votedFor = null, leaderId = proposedLeaderId)
    }

    fun updateCommitMetadata(appliedLogIndex: Long, appliedLogTerm: Long, commitIndex: Long) {
        commitMetadata = commitMetadata.copy(
            lastAppliedLogIndex = appliedLogIndex,
            lastAppliedLogTerm = appliedLogTerm,
            commitIndex = commitIndex
        )
    }

    fun updateAsFollower(operationTerm: Long) {
        logger.info("Updating node state as follower with term=[{}]", operationTerm)
        val currentTerm = if (consensusMetadata.currentTerm < operationTerm) {
            logger.info("Updating term with term=[{}] and resetting votedFor", operationTerm)
            operationTerm
        } else {
            consensusMetadata.currentTerm
        }
        consensusMetadata = consensusMetadata.copy(votedFor = null, currentTerm = currentTerm, mode = NodeMode.FOLLOWER)
    }

    fun updateAsCandidate() {
        logger.info("Updating node state as candidate")
        consensusMetadata = consensusMetadata.copy(
            currentTerm = consensusMetadata.currentTerm + 1,
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
