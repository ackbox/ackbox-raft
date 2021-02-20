package com.ackbox.raft.core

import com.ackbox.raft.api.LeaderNode
import com.ackbox.raft.api.LeaderNode.AddNode
import com.ackbox.raft.api.LeaderNode.AddNode.Output
import com.ackbox.raft.api.LeaderNode.GetItem
import com.ackbox.raft.api.LeaderNode.SetItem
import com.ackbox.raft.api.ReplicaNode
import com.ackbox.raft.api.ReplicaNode.Append
import com.ackbox.raft.api.ReplicaNode.Vote
import com.ackbox.raft.config.NodeConfig
import com.ackbox.raft.networking.NodeInmemoryAddress
import com.ackbox.raft.statemachine.Snapshot
import com.ackbox.raft.support.Callback
import com.ackbox.raft.support.CommitIndexMismatchException
import com.ackbox.raft.support.NodeLogger
import com.ackbox.raft.support.NotLeaderException
import com.ackbox.raft.support.ReplicaStateMismatchException
import com.ackbox.raft.support.ReplyTermInvariantException
import com.ackbox.raft.support.TemporaryFile
import com.ackbox.raft.types.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import java.time.Duration
import javax.annotation.concurrent.ThreadSafe

/**
 * Raft node API implementation. The behavior implemented here follows that paragraphs $5.1, $5.2, $5.3 and $5.4 of the
 * original paper. The name of the variables may have been slightly modified for better clarity. However, most of the
 * logic follows closely the proposed algorithm in the paper.
 *
 * @see [Raft Paper](https://raft.github.io/raft.pdf)
 */
@ThreadSafe
class LocalNode(
    private val config: NodeConfig,
    private val locked: LocalNodeState,
    private val remotes: RemoteNodes
) : LeaderNode, ReplicaNode {

    private val logger: NodeLogger = NodeLogger.from(config.nodeId, LocalNode::class)

    override val nodeId: String = config.nodeId

    fun start() {
        val electionCallback = createElectionCallback()
        val heartbeatCallback = createHeartbeatCallback()
        val snapshotCallback = createSnapshotCallback()
        remotes.start()
        locked.withLock(START_NODE_OPERATION) { state ->
            state.start(electionCallback, heartbeatCallback, snapshotCallback)
        }
    }

    fun stop(timeout: Duration) {
        locked.withLock(STOP_NODE_OPERATION) { state -> state.stop() }
        remotes.stop(timeout)
    }

    fun describeState() {
        locked.withLock(DESCRIBE_STATE_OPERATION) { state -> state.log.describe() }
    }

    override fun setItem(input: SetItem.Input): SetItem.Output {
        return locked.withLock(REQUEST_APPEND_OPERATION) { state ->
            // Check whether the current node is the leader. If not, simply fail the request letting caller know who is
            // the leader for the current term.
            val consensusMetadata = state.metadata.consensusMetadata
            val leaderId = consensusMetadata.leaderId
            if (consensusMetadata.mode != NodeMode.LEADER) {
                throw NotLeaderException(leaderId)
            }

            // Apply entries the replicated to the leader's log before doing anything.
            val leaderTerm = consensusMetadata.currentTerm
            val items = convertToLogItems(leaderTerm, state.log.getLastItemIndex(), input.data)
            val lastLogItemIndex = state.appendLogItems(items)

            // Dispatch requests in parallel to all replicas/remote in the cluster in order to append the new entry to
            // their logs. In these requests, we send the current leader information (id and term) as well as pack all
            // entries that we think the remote node will need to catch up with leader's log. In order to do so, we use
            // the remote#nextLogIndex and remote#matchLogIndex information.
            val remoteStates = try {
                remotes.appendItems(state.metadata, state.log, state.getLatestSnapshot())
            } catch (e: ReplyTermInvariantException) {
                // Remotes will throw term invariant exception whenever they detect that their term is greater than the
                // node that generated the append request (case of 'this' node since it's performing the append
                // request). This might mean that there are multiple nodes thinking they are leaders in the cluster.
                state.transitionToFollower(e.remoteTerm)
                throw e
            }

            // Verify replication consensus status across majority in the cluster.
            val matchIndexes = mutableListOf<Long>()
                .apply { remoteStates.forEach { add(it.matchLogIndex.value) } }
                .apply { add(lastLogItemIndex.value) }
                .sorted()
            val commitIndex = Index(matchIndexes[remotes.remotesCount / 2])
            val commitMetadata = state.metadata.commitMetadata
            logger.info("Commit index updated from [{}] to [{}]", commitMetadata.commitIndex, commitIndex)

            // Only update the commit index if there's consensus in the replication results from all the remotes in the
            // cluster.
            if (commitIndex < commitMetadata.commitIndex) {
                throw CommitIndexMismatchException(leaderId, commitMetadata.commitIndex, commitIndex)
            }

            // Commit items that were confirmed to be replicated across the cluster.
            state.commitLogItems(commitIndex)
            return@withLock SetItem.Output(leaderId)
        }
    }

    override fun getItem(input: GetItem.Input): GetItem.Output {
        return locked.withLock(REQUEST_RETRIEVE_OPERATION) { state ->
            // Check whether the current node is the leader. If not, simply fail the request letting caller know who is
            // the leader for the current term.
            val consensusMetadata = state.metadata.consensusMetadata
            val leaderId = consensusMetadata.leaderId
            if (consensusMetadata.mode != NodeMode.LEADER) {
                throw NotLeaderException(leaderId)
            }

            // Retrieve committed entry from state machine.
            GetItem.Output(leaderId, state.getStateValue(input.key))
        }
    }

    override fun addNode(input: AddNode.Input): Output {
        val snapshot = locked.withLock(REQUEST_ADD_NODE_OPERATION) { state ->
            // Check whether the current node is the leader. If not, simply fail the request letting caller know who is
            // the leader for the current term.
            val consensusMetadata = state.metadata.consensusMetadata
            if (consensusMetadata.mode != NodeMode.LEADER) {
                throw NotLeaderException(consensusMetadata.leaderId)
            }
            return@withLock state.getLatestSnapshot()
        }

        // Try to activate the new node by sending the latest snapshot.
        val leaderId = remotes.activateNode(input.address, snapshot) {
            // Finalize the node activation by broadcasting the new node to all replicas in the cluster.
            val output = setItem(SetItem.Input(type = EntryType.ADD_NODE_CHANGE, payload = payload))
            return@activateNode output.leaderId
        }
        return Output(leaderId)
    }

    override suspend fun handleAppend(input: Append.Input): Append.Output {
        // We ensure the issuer of the request is a node that we know about.
        remotes.ensureValidRemote(input.leaderId)

        // Lock the state in order to avoid inconsistencies while checks and validations are being performed for the
        // append request.
        return locked.withLock(HANDLE_APPEND_OPERATION) { state ->
            // Check whether there are multiple leaders sending append requests. If multiple leaders are detected, force
            // them to step down and restart an election by incrementing the term.
            val leaderId = input.leaderId
            val leaderTerm = input.leaderTerm

            // We ensure a valid leader by checking the following cases:
            //   1. The current node has a smaller currentTerm than leaderTerm.
            //   2. The current node has no leader assigned to it and its currentTerm is less than leaderTerm.
            //   3. The current node has a leader assigned to it and leaderId matches with this metadata.
            state.ensureValidLeader(leaderId, leaderTerm)

            // Register the reception of a message from the leader so that internal timers can be updated.
            state.refreshLeaderInformation(leaderId, leaderTerm)

            // Check whether the node's replicated log matches the leader's. Two conditions are tested:
            //   1. The node's log should contain an entry at previousLogIndex.
            //   2. The entry mapped to previousLogIndex should match the term previousLogTerm.
            // We may have noticed that we do not try to check the logs are exactly in sync. The only necessary
            // guarantee is that the node's and leader's log agree at the point previousLogIndex. We do not care if the
            // node's log is ahead. We just make sure that node's log is not behind. The leader will then made the
            // necessary adjustments in order to fix inconsistencies. For instance, entries will be retransmitted until
            // the logs are synchronized.
            val currentTerm = state.metadata.consensusMetadata.currentTerm
            val previousLogIndex = input.previousLogIndex
            val previousLogTerm = input.previousLogTerm
            val isFirstAppend = previousLogIndex.isUndefined() && previousLogTerm.isUndefined()
            if (!isFirstAppend && !state.log.containsItem(previousLogIndex, previousLogTerm)) {
                logger.info("Log mismatch for logIndex=[{}] and logTerm=[{}]", previousLogIndex, previousLogTerm)
                // Optimization can be done in order to ensure minimum retransmission. Instead of returning
                // previousLogIndex - 1 for the case of index mismatch, we can return state.log.getLastItemIndex().
                val lastLogItem = state.log.getItem(state.log.getLastItemIndex())!!
                val lastLogIndex = lastLogItem.index
                val correctionLogIndex =
                    if (lastLogIndex == previousLogIndex) previousLogIndex.decremented() else lastLogIndex
                throw ReplicaStateMismatchException(currentTerm, correctionLogIndex)
            }

            // The node can now safely apply the log entries to its log.
            logger.info("Append entry ready to be committed until index=[{}]", input.leaderCommitIndex)
            val leaderCommitIndex = input.leaderCommitIndex
            val processedLastLogItemIndex = state.appendLogItems(input.items)
            state.commitLogItems(leaderCommitIndex)
            remotes.activateNode(NodeInmemoryAddress(""))
            return@withLock Append.Output(currentTerm, processedLastLogItemIndex)
        }
    }

    override suspend fun handleVote(input: Vote.Input): Vote.Output {
        // We ensure the issuer of the request is a node that we know about.
        remotes.ensureValidRemote(input.candidateId)

        // Lock the state in order to avoid inconsistencies while checks and validations are being performed for the
        // vote request.
        return locked.withLock(HANDLE_VOTE_OPERATION) { state ->
            // We ensure a valid candidate by checking the following cases:
            //   1. Check whether the candidate node has a term greater than the term known by this node.
            //   2. Check whether the node can vote for the candidate.
            //   3. Check whether the candidate node's log is caught up with the current node's log.
            state.ensureValidCandidate(input.candidateId, input.candidateTerm, input.lastLogIndex, input.lastLogTerm)

            // In order to grant a vote, two conditions should be met:
            //   1. No vote has been issued or vote issued == candidateId.
            //   2. Candidate's log is caught up with node's.
            // Note that the log consistency restriction allows for a uni-directional flow for entries: entries flow
            // from leader to follower always. A leader will never be elected if its log is behind.
            logger.info("Vote granted to candidateId=[{}]", input.candidateId)
            state.updateVote(input.candidateId)
            return@withLock Vote.Output(state.metadata.consensusMetadata.currentTerm)
        }
    }

    @Suppress("BlockingMethodInNonBlockingContext")
    override suspend fun handleSnapshot(inputs: Flow<ReplicaNode.Snapshot.Input>): ReplicaNode.Snapshot.Output {
        return TemporaryFile("snapshot").use { file ->
            var lastInput: ReplicaNode.Snapshot.Input? = null
            // Perform some validations while reading the stream in order to avoid waiting for the whole process to
            // complete. This is a fail fast mechanism and it does not mean the snapshot will succeed if these primary
            // validations are successful.
            val ensureValidSnapshot = fun(input: ReplicaNode.Snapshot.Input) {
                // We ensure the issuer of the request is a node that we know about.
                remotes.ensureValidRemote(input.leaderId)

                // We ensure the request is being issued by the node we consider the leader.
                val leaderId = input.leaderId
                val leaderTerm = input.leaderTerm
                locked.withLock(HANDLE_SNAPSHOT_OPERATION) { state ->
                    state.ensureValidLeader(leaderId, leaderTerm)
                }
            }
            file.outputStream().use { output ->
                inputs.collect { input ->
                    ensureValidSnapshot(input)
                    output.write(input.partial.array())
                    lastInput = input
                }
            }
            check(lastInput != null) { "Last snapshot request cannot be null" }

            // A snapshot request is sent by the leader when it detects the replica is behind the acceptable threshold.
            // If the state drift between the leader and the replica is small, the period heartbeat between the leader
            // and the replica is sufficient for the replica to reconstruct its state.
            return@use locked.withLock(HANDLE_SNAPSHOT_OPERATION) { state ->
                val leaderId = lastInput!!.leaderId
                val leaderTerm = lastInput!!.leaderTerm

                // We ensure a valid leader by checking the following cases:
                //   1. The current node has a smaller currentTerm than leaderTerm.
                //   2. The current node has no leader assigned to it and its currentTerm is less than leaderTerm.
                //   3. The current node has a leader assigned to it and leaderId matches with this metadata.
                state.ensureValidLeader(leaderId, leaderTerm)

                // Adjust state machine and log according to the snapshot provided by the leader. The log will be
                // trimmed up until [input.lastIncludedLogIndex] and the state machine's state will be replaced by the
                // contents of the snapshot file.
                val lastIncludedLogIndex = lastInput!!.lastIncludedLogIndex
                val lastIncludedLogTerm = lastInput!!.lastIncludedLogTerm
                val snapshot = Snapshot(lastIncludedLogIndex, lastIncludedLogTerm, file.toPath())
                state.restoreSnapshot(snapshot)

                logger.info("Snapshot successfully loaded for input=[{}]", lastInput)
                return@withLock ReplicaNode.Snapshot.Output(state.metadata.consensusMetadata.currentTerm)
            }
        }
    }

    private fun createElectionCallback(): Callback {
        return {
            logger.info("Starting a new election")
            // Collect votes from remotes in the cluster. If we detect a violation of the term invariant, we stop the
            // process and transition the current node to follower mode.
            val votes = locked.withLock(REQUEST_VOTE_OPERATION) { state ->
                state.transitionToCandidate()
                try {
                    remotes.requestVote(state.metadata, state.log)
                } catch (e: ReplyTermInvariantException) {
                    state.transitionToFollower(e.remoteTerm)
                    throw e
                } catch (e: Exception) {
                    val consensusMetadata = state.metadata.consensusMetadata
                    state.transitionToFollower(consensusMetadata.currentTerm)
                    throw e
                }
            }
            // Check whether candidate node won the election. Here we add '1' to total to take into account the vote of
            // the current node, which of course is voting for itself.
            locked.withLock(REQUEST_VOTE_OPERATION) { state ->
                val total = 1 + votes.map { if (it) 1 else 0 }.sum()
                if (total > remotes.remotesCount / 2) {
                    logger.info("Node has been elected as leader")
                    remotes.resetState(state.log.getFirstItemIndex())
                    state.transitionToLeader()
                } else {
                    logger.info("Node is transitioning to follower due to not enough votes")
                    val consensusMetadata = state.metadata.consensusMetadata
                    state.transitionToFollower(consensusMetadata.currentTerm)
                }
            }
        }
    }

    private fun createHeartbeatCallback(): Callback {
        return {
            try {
                logger.info("Started heartbeat with followers")
                val output = setItem(SetItem.Input(emptyList()))
                logger.debug("Finished heartbeat with followers: [{}]", output)
            } catch (e: Exception) {
                logger.error("Error while executing heartbeat", e)
            }
        }
    }

    private fun createSnapshotCallback(): Callback {
        return {
            try {
                logger.info("Taking a new snapshot of node's state")
                val snapshot = locked.withLock(REQUEST_SNAPSHOT_OPERATION) { state ->
                    state.takeSnapshot()
                    state.getLatestSnapshot()
                }
                logger.info("Finished taking snapshot: [{}]", snapshot)
            } catch (e: Exception) {
                logger.error("Error while executing snapshot", e)
            }
        }
    }

    private fun convertToLogItems(currentTerm: Term, lastItemIndex: Index, data: List<ByteArray>): List<LogItem> {
        return data.mapIndexed { index, bytes ->
            LogItem(lastItemIndex.incrementedBy(index.toLong()), currentTerm, bytes)
        }
    }

    companion object {

        fun fromConfig(config: NodeConfig, remotes: RemoteNodes): LocalNode {
            return LocalNode(config, LocalNodeState.fromConfig(config), remotes)
        }
    }
}
