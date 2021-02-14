package com.ackbox.raft.state

import com.ackbox.raft.config.NodeConfig
import com.ackbox.raft.log.ReplicatedLog
import com.ackbox.raft.log.ReplicatedLog.LogItem
import com.ackbox.raft.log.SegmentedLog
import com.ackbox.raft.statemachine.ReplicatedStateMachine
import com.ackbox.raft.statemachine.SimpleStateMachine
import com.ackbox.raft.support.LeaderMismatchException
import com.ackbox.raft.support.NodeLogger
import com.ackbox.raft.support.NodeTimer
import com.ackbox.raft.support.RequestTermInvariantException
import com.ackbox.raft.support.VoteNotGrantedException
import java.util.UUID
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.math.min

typealias Callback = () -> Unit

/**
 * State for the current local node. This class encapsulates an instance of [UnsafeLocalNodeState].
 * It only allows external object to modify [UnsafeLocalNodeState] under a lock to prevent issues
 * due to concurrent mutations.
 */
class LocalNodeState(val nodeId: String, private val unsafeState: UnsafeLocalNodeState) {

    private val logger: NodeLogger = NodeLogger.from(nodeId, LocalNodeState::class)
    private val lock: Lock = ReentrantLock()

    fun <T : Any> withLock(operationName: String, function: (UnsafeLocalNodeState) -> T): T {
        val operationId = UUID.randomUUID().toString()
        return lock.withLock {
            logger.info("Locked for operation [{}::{}]", operationName, operationId)
            val result = function(unsafeState)
            logger.info("Unlocked for operation [{}::{}]", operationName, operationId)
            result
        }
    }

    companion object {

        fun fromConfig(config: NodeConfig): LocalNodeState {
            return LocalNodeState(config.nodeId, UnsafeLocalNodeState(config))
        }
    }
}

class UnsafeLocalNodeState(
    private val config: NodeConfig,
    val metadata: Metadata = Metadata(config.nodeId),
    val log: ReplicatedLog = SegmentedLog(config),
    val stateMachine: ReplicatedStateMachine = SimpleStateMachine(config.nodeId)
) {

    private val logger: NodeLogger = NodeLogger.from(config.nodeId, UnsafeLocalNodeState::class)
    private val timer: NodeTimer = NodeTimer(config)

    private var electionCallback: Callback? = null
    private var heartbeatCallback: Callback? = null

    fun start(electionCallback: Callback, heartbeatCallback: Callback) {
        logger.info("Loading state from persistent storage")
        log.open()
        val lastLogItem = log.getItem(log.getLastItemIndex())
        lastLogItem?.let { metadata.updateAsFollower(it.term) }

        logger.info("Starting state timers")
        this.electionCallback = electionCallback
        this.heartbeatCallback = heartbeatCallback
        timer.restartElectionTimer(electionCallback)
    }

    fun stop() {
        logger.info("Stopping state timers")
        timer.stopElectionTimer()
        timer.stopHeartbeatTimer()

        logger.info("Saving state from persistent storage")
        log.close()
    }

    fun ensureValidLeader(leaderId: String, leaderTerm: Long) {
        val currentTerm = metadata.currentTerm
        val currentLeaderId = metadata.leaderId
        if (currentTerm > leaderTerm) {
            // Check whether the node that thinks it is the leader really has a term greater than the
            // term known by this node. In this case, reject the request and let the issuer of the request
            // know that there is a node in the cluster with a greater term.
            logger.info("Term mismatch: currentTerm=[{}], leaderTerm=[{}]", currentTerm, leaderTerm)
            throw RequestTermInvariantException(currentTerm, leaderTerm, log.getLastItemIndex())
        } else if (metadata.canAcceptLeader(leaderId) && leaderTerm > currentTerm) {
            // No leader set yet, so simply accept the current leader.
            logger.info("Accepting current leader: leaderId=[{}], term=[{}]", leaderId, leaderTerm)
            refreshLeaderInformation(leaderId, leaderTerm)
        } else if (!metadata.matchesLeaderId(leaderId)) {
            // Force caller node to step down by incrementing the term.
            logger.info("Multiple leaders detected: leaderId=[{}], otherId=[{}]", currentLeaderId, leaderId)
            transitionToFollower(currentTerm + 1)
            throw LeaderMismatchException(currentLeaderId, currentTerm + 1, log.getLastItemIndex())
        }
    }

    fun ensureValidCandidate(candidateId: String, candidateTerm: Long, lastLogIndex: Long, lastLogTerm: Long) {
        // Check whether the candidate node has a term greater than the term known by this node.
        val currentTerm = metadata.currentTerm
        if (currentTerm > candidateTerm) {
            logger.info("Term mismatch: currentTerm=[{}] and candidateTerm=[{}]", currentTerm, candidateTerm)
            throw RequestTermInvariantException(currentTerm, candidateTerm, log.getLastItemIndex())
        }

        // Check whether the node can vote for the candidate. That means the node hasn't voted yet
        // or has voted to the same candidateId.
        if (!metadata.canAcceptLeader(candidateId)) {
            logger.info("Vote not granted to candidateId=[{}] - (reason: cannot vote)", candidateId)
            throw VoteNotGrantedException(candidateId, currentTerm)
        }

        // Check whether the candidate node's log is caught up with the current node's log.
        if (log.isAheadOf(lastLogIndex, lastLogTerm)) {
            logger.info("Vote not granted to candidateId=[{}] - (reason: candidate log is behind)", candidateId)
            throw VoteNotGrantedException(candidateId, currentTerm)
        }
    }

    fun appendLogItems(items: List<LogItem>): Long {
        log.appendItems(items)
        return log.getLastItemIndex()
    }

    fun commitLogItems(leaderCommitIndex: Long) {
        // If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry).
        var commitIndex = metadata.commitIndex
        if (leaderCommitIndex > commitIndex) {
            commitIndex = min(leaderCommitIndex, log.getLastItemIndex())
        }
        // If commitIndex > lastApplied, increment lastApplied, apply log[lastApplied] to state machine.
        val lastAppliedLogIndex = metadata.lastAppliedLogIndex
        if (commitIndex > lastAppliedLogIndex) {
            val itemApplyRange = lastAppliedLogIndex..commitIndex
            itemApplyRange.forEach { index ->
                log.getItem(index)?.let { stateMachine.setItem(it) }
                // Update lastAppliedLogIndex to currently applied item index.
                metadata.updateLastAppliedLogIndex(commitIndex, index)
            }
        }
    }

    fun getCommittedLogItem(index: Long): LogItem? {
        return stateMachine.getItem(index)
    }

    fun updateVote(candidateId: String) {
        metadata.updateVote(candidateId)
    }

    fun refreshLeaderInformation(leaderId: String, leaderTerm: Long) {
        transitionToFollower(leaderTerm)
        metadata.updateLeaderId(leaderId)
    }

    fun transitionToCandidate() {
        metadata.updateAsCandidate()
        timer.stopHeartbeatTimer()
        timer.stopElectionTimer()
    }

    fun transitionToFollower(operationTerm: Long) {
        metadata.updateAsFollower(operationTerm)
        timer.stopHeartbeatTimer()
        timer.restartElectionTimer(electionCallback)
    }

    fun transitionToLeader() {
        metadata.updateAsLeader()
        timer.stopElectionTimer()
        timer.restartHeartbeatTimer(heartbeatCallback)
    }
}
