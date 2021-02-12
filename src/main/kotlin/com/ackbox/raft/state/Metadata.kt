package com.ackbox.raft.state

import com.ackbox.raft.core.UNDEFINED_ID
import com.ackbox.raft.networking.NodeNetworking
import com.ackbox.raft.support.NodeLogger

class Metadata(
    /**
     * Unique identifier for the current node.
     */
    val nodeId: String,

    /**
     * Latest term server has seen (initialized to 0 on first boot, increases monotonically).
     */
    private var currentTerm: Long = UNDEFINED_ID,

    /**
     * CandidateId that received vote in current term (or null if none).
     */
    private var votedFor: String? = null,

    /**
     * NodeId that is said to be the leader in current term (or null if none).
     */
    private var leaderId: String? = null,

    /**
     * Index of highest log entry known to be committed (initialized to 0, increases monotonically).
     */
    private var commitIndex: Long = UNDEFINED_ID,

    /**
     * Index of highest log entry applied to state machine (initialized to 0, increases monotonically)
     */
    private var lastAppliedLogIndex: Long = UNDEFINED_ID,

    /**
     * Current operation mode of the node in the term.
     */
    private var mode: NodeMode = NodeMode.FOLLOWER
) {

    private val logger = NodeLogger.from(nodeId, NodeNetworking::class)

    enum class NodeMode { FOLLOWER, CANDIDATE, LEADER }

    fun getCurrentTerm(): Long = currentTerm

    fun getLeaderId(): String? = leaderId

    fun getCommitIndex(): Long = commitIndex

    fun getLastAppliedLogIndex(): Long = lastAppliedLogIndex

    fun getMode(): NodeMode = mode

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
}
