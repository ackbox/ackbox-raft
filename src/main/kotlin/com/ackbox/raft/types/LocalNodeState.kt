package com.ackbox.raft.types

import com.ackbox.raft.config.NodeConfig
import com.ackbox.raft.log.ReplicatedLog
import com.ackbox.raft.log.SegmentedLog
import com.ackbox.raft.statemachine.KeyValueStore
import com.ackbox.raft.statemachine.ReplicatedStateMachine
import com.ackbox.raft.statemachine.Snapshot
import com.ackbox.raft.support.Callback
import com.ackbox.raft.support.LeaderMismatchException
import com.ackbox.raft.support.LockNotAcquiredException
import com.ackbox.raft.support.NodeLogger
import com.ackbox.raft.support.NodeTimer
import com.ackbox.raft.support.RequestTermInvariantException
import com.ackbox.raft.support.VoteNotGrantedException
import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock
import javax.annotation.concurrent.NotThreadSafe
import javax.annotation.concurrent.ThreadSafe

/**
 * State for the current local node. This class encapsulates an instance of [UnsafeLocalNodeState]. It only allows
 * external object to modify [UnsafeLocalNodeState] under a lock to prevent issues due to concurrent mutations.
 */
@ThreadSafe
class LocalNodeState(private val config: NodeConfig, private val unsafeState: UnsafeLocalNodeState) {

    private val logger: NodeLogger = NodeLogger.from(config.nodeId, LocalNodeState::class)
    private val lock: Lock = ReentrantLock()

    fun <T : Any> withLock(operationName: String, function: (UnsafeLocalNodeState) -> T): T {
        val operationId = UUID.randomUUID().toString()
        return try {
            // In order to avoid deadlocks, we fail fast if we are unable to acquire the lock to safely modify the
            // node's state. Requests are supposed to be retried once they fail with this exception.
            if (!lock.tryLock(config.maxStateLockWaitTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
                throw LockNotAcquiredException()
            }
            logger.info("Locked for operation [{}::{}]", operationName, operationId)
            val result = function(unsafeState)
            result
        } finally {
            logger.info("Unlocked for operation [{}::{}]", operationName, operationId)
            lock.unlock()
        }
    }

    companion object {

        fun fromConfig(config: NodeConfig): LocalNodeState {
            return LocalNodeState(config, UnsafeLocalNodeState(config))
        }
    }
}

@NotThreadSafe
class UnsafeLocalNodeState(
    private val config: NodeConfig,
    private val stateMachine: ReplicatedStateMachine = KeyValueStore(config),
    val metadata: Metadata = Metadata(config.nodeId),
    val log: ReplicatedLog = SegmentedLog(config)
) {

    private val logger: NodeLogger = NodeLogger.from(config.nodeId, UnsafeLocalNodeState::class)
    private val timer: NodeTimer = NodeTimer(config)

    private var snapshot: Snapshot = Snapshot.load(config.snapshotPath)
    private var electionCallback: Callback? = null
    private var heartbeatCallback: Callback? = null

    fun start(electionCallback: Callback, heartbeatCallback: Callback, snapshotCallback: Callback) {
        logger.info("Loading state from persistent storage")
        loadState()

        logger.info("Starting state timers")
        this.electionCallback = electionCallback
        this.heartbeatCallback = heartbeatCallback
        timer.restartElectionTimer(electionCallback)
        timer.restartSnapshotTimer(snapshotCallback)
    }

    fun stop() {
        logger.info("Stopping state timers")
        timer.stopAll()

        logger.info("Saving state from persistent storage")
        log.close()
    }

    fun ensureValidLeader(leaderId: String, leaderTerm: Term) {
        val currentTerm = metadata.consensusMetadata.currentTerm
        val currentLeaderId = metadata.consensusMetadata.leaderId
        if (currentTerm > leaderTerm) {
            // Check whether the node that thinks it is the leader really has a term greater than the term known by this
            // node. In this case, reject the request and let the issuer of the request know that there is a node in the
            // cluster with a greater term.
            logger.info("Term mismatch: currentTerm=[{}], leaderTerm=[{}]", currentTerm, leaderTerm)
            throw RequestTermInvariantException(currentTerm, leaderTerm, log.getLastItemIndex())
        } else if (metadata.canAcceptLeader(leaderId) && leaderTerm > currentTerm) {
            // No leader set yet, so simply accept the current leader.
            logger.info("Accepting current leader: leaderId=[{}], term=[{}]", leaderId, leaderTerm)
            refreshLeaderInformation(leaderId, leaderTerm)
        } else if (!metadata.matchesLeaderId(leaderId)) {
            // Force caller node to step down by incrementing the term.
            logger.info("Multiple leaders detected: leaderId=[{}], otherId=[{}]", currentLeaderId, leaderId)
            transitionToFollower(currentTerm.incremented())
            throw LeaderMismatchException(currentLeaderId, currentTerm.incremented(), log.getLastItemIndex())
        }
    }

    fun ensureValidCandidate(candidateId: String, candidateTerm: Term, lastLogIndex: Index, lastLogTerm: Term) {
        // Check whether the candidate node has a term greater than the term known by this node.
        val currentTerm = metadata.consensusMetadata.currentTerm
        if (currentTerm > candidateTerm) {
            logger.info("Term mismatch: currentTerm=[{}] and candidateTerm=[{}]", currentTerm, candidateTerm)
            throw RequestTermInvariantException(currentTerm, candidateTerm, log.getLastItemIndex())
        }

        // Check whether the node can vote for the candidate. That means the node hasn't voted yet or has voted to the
        // same candidateId.
        if (!metadata.canAcceptLeader(candidateId)) {
            logger.info("Vote not granted to candidateId [{}] - (reason: cannot vote)", candidateId)
            throw VoteNotGrantedException(candidateId, currentTerm)
        }

        // Check whether the candidate node's log is caught up with the current node's log.
        if (log.isAheadOf(lastLogIndex, lastLogTerm)) {
            logger.info("Vote not granted to candidateId [{}] - (reason: candidate log is behind)", candidateId)
            throw VoteNotGrantedException(candidateId, currentTerm)
        }
    }

    fun appendLogItems(items: List<LogItem>): Index {
        log.appendItems(items)
        return log.getLastItemIndex()
    }

    fun commitLogItems(leaderCommitIndex: Index) {
        // If leaderCommitIndex > commitIndex, set commitIndex = min(leaderCommitIndex, index of last new item).
        val commitMetadata = metadata.commitMetadata
        var commitIndex = commitMetadata.commitIndex
        if (leaderCommitIndex > commitIndex) {
            commitIndex = min(leaderCommitIndex, log.getLastItemIndex())
        }
        logger.info("Set commitIndex to [{}] from leaderCommitIndex [{}]", commitIndex, leaderCommitIndex)
        // If commitIndex > lastApplied, increment lastApplied, apply log[lastApplied] to state machine.
        val lastAppliedLogIndex = commitMetadata.lastAppliedLogIndex
        if (commitIndex > lastAppliedLogIndex) {
            (lastAppliedLogIndex.value..commitIndex.value).forEach { index ->
                val item = log.getItem(Index(index)) ?: throw IllegalStateException("Corrupted log at index [$index]")
                stateMachine.setValue(item.value)
                // Update lastAppliedLogIndex to currently applied item index.
                metadata.updateCommitMetadata(item.index, item.term, commitIndex)
            }
        }
    }

    fun getStateValue(key: String): ByteBuffer? {
        return stateMachine.getValue(key)?.let { ByteBuffer.wrap(it) }
    }

    fun restoreSnapshot(newSnapshot: Snapshot) {
        // We try to find an item in the current node's log that matches the index and term from the snapshot provided.
        // If the item is found, it means that the log matching property is satisfied, thus all subsequent items
        // (if existent) are compatible with the leader's.
        if (log.containsItem(newSnapshot.lastIncludedLogIndex, newSnapshot.lastIncludedLogTerm)) {
            return
        }

        // If no item is found, we replace the state machine's state with the snapshot provided as well as reset the
        // logs so that the leader can properly replicate its own.
        stateMachine.restoreSnapshot(newSnapshot.dataPath)
        metadata.updateCommitMetadata(
            newSnapshot.lastIncludedLogIndex,
            newSnapshot.lastIncludedLogTerm,
            newSnapshot.lastIncludedLogIndex
        )
        log.clear()

        // Up until here, the snapshot was in a temporary folder on the file system. If all the previous operations are
        // successful, we promote the snapshot to latest.
        snapshot = newSnapshot.save(config.snapshotPath)
    }

    fun takeSnapshot() {
        val dataPath = stateMachine.takeSnapshot()
        val commitMetadata = metadata.commitMetadata
        val newSnapshot = Snapshot(commitMetadata.lastAppliedLogIndex, commitMetadata.lastAppliedLogTerm, dataPath)
        log.truncateBeforeNonInclusive(commitMetadata.lastAppliedLogIndex)

        // Up until here, the snapshot was in a temporary folder on the file system. If all the previous operations are
        // successful, we promote the snapshot to latest.
        snapshot = newSnapshot.save(config.snapshotPath)
    }

    fun getLatestSnapshot(): Snapshot {
        return Snapshot.load(config.snapshotPath)
    }

    fun updateVote(candidateId: String) {
        metadata.updateVote(candidateId)
    }

    fun refreshLeaderInformation(leaderId: String, leaderTerm: Term) {
        transitionToFollower(leaderTerm)
        metadata.updateLeaderId(leaderId)
    }

    fun transitionToCandidate() {
        metadata.updateAsCandidate()
        timer.stopHeartbeatTimer()
        timer.stopElectionTimer()
    }

    fun transitionToFollower(operationTerm: Term) {
        metadata.updateAsFollower(operationTerm)
        timer.stopHeartbeatTimer()
        timer.restartElectionTimer(electionCallback)
    }

    fun transitionToLeader() {
        metadata.updateAsLeader()
        timer.stopElectionTimer()
        timer.restartHeartbeatTimer(heartbeatCallback)
    }

    private fun loadState() {
        logger.info("Loading state using snapshot [{}]", snapshot)

        // Restore state from the latest snapshot available. It is ok if no snapshot is available. By default the,
        // whenever no snapshot is available, a snapshot object is initialized with the default values for
        // lastIncludedLogIndex and lastIncludedLogTerm.
        val lastLogItem = log.getItem(log.getLastItemIndex())
        val index = max(snapshot.lastIncludedLogIndex, lastLogItem?.index ?: Index())
        val term = max(snapshot.lastIncludedLogTerm, lastLogItem?.term ?: Term())
        val commitIndex = snapshot.lastIncludedLogIndex
        stateMachine.restoreSnapshot(snapshot.dataPath)

        // Load node's log and trim it before the snapshot's last included log index to save some storage space.
        log.open()
        log.truncateBeforeNonInclusive(snapshot.lastIncludedLogIndex)

        // Update node's metadata according to the snapshot metadata and transition to follower mode.
        metadata.updateCommitMetadata(index, term, commitIndex)
        metadata.updateAsFollower(term)

        // Commit any log item that can be committed.
        commitLogItems(commitIndex)
    }
}
