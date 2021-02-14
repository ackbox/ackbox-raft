package com.ackbox.raft.state

import com.ackbox.raft.core.UNDEFINED_ID
import com.ackbox.raft.networking.NodeNetworking
import com.ackbox.raft.support.NodeLogger

/**
 * Class defining basic metadata of the algorithm.
 * @param nodeId Unique identifier for the current node.
 */
class Metadata(val nodeId: String) {
    /**
     * Latest term server has seen (initialized to 0 on first boot, increases monotonically).
     */
    var currentTerm: Long = UNDEFINED_ID
        private set

    /**
     * NodeId that is said to be the leader in current term (or null if none).
     */
    var leaderId: String? = null
        private set

    /**
     * Index of highest log entry known to be committed (initialized to 0, increases monotonically).
     */
    var commitIndex: Long = UNDEFINED_ID
        private set

    /**
     * Index of highest log entry applied to state machine (initialized to 0, increases monotonically)
     */
    var lastAppliedLogIndex: Long = UNDEFINED_ID
        private set

    /**
     * Current operation mode of the node in the term.
     */
    var mode: NodeMode = NodeMode.FOLLOWER
        private set

    /**
     * CandidateId that received vote in current term (or null if none).
     */
    private var votedFor: String? = null

    private val logger = NodeLogger.from(nodeId, NodeNetworking::class)

    fun canAcceptLeader(candidateId: String): Boolean {
        return votedFor == null || votedFor == candidateId
    }

    fun matchesLeaderId(proposedLeaderId: String): Boolean {
        return leaderId == null || leaderId == proposedLeaderId
    }

    fun updateVote(candidateId: String) {
        logger.info("Updating vote for candidate=[{}]", candidateId)
        votedFor = candidateId
    }

    fun updateLeaderId(proposedLeaderId: String) {
        logger.info("Updating leader to leaderId=[{}]", proposedLeaderId)
        leaderId = proposedLeaderId
        votedFor = null
    }

    fun updateLastAppliedLogIndex(newCommitIndex: Long, index: Long) {
        commitIndex = newCommitIndex
        lastAppliedLogIndex = index
    }

    fun updateAsFollower(operationTerm: Long) {
        logger.info("Updating node state as follower with term=[{}]", operationTerm)
        if (currentTerm < operationTerm) {
            logger.info("Updating term with term=[{}] and resetting votedFor", operationTerm)
            currentTerm = operationTerm
        }
        votedFor = null
        mode = NodeMode.FOLLOWER
    }

    fun updateAsCandidate() {
        logger.info("Updating node state as candidate")
        currentTerm++
        leaderId = null
        votedFor = nodeId
        mode = NodeMode.CANDIDATE
    }

    fun updateAsLeader() {
        logger.info("Updating node state as leader")
        leaderId = nodeId
        votedFor = nodeId
        mode = NodeMode.LEADER
    }

    enum class NodeMode { FOLLOWER, CANDIDATE, LEADER }
}
