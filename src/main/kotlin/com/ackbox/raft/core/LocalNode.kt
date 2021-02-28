package com.ackbox.raft.core

import com.ackbox.raft.api.LeaderNode
import com.ackbox.raft.api.LeaderNode.AddNode
import com.ackbox.raft.api.LeaderNode.GetEntry
import com.ackbox.raft.api.LeaderNode.RemoveNode
import com.ackbox.raft.api.LeaderNode.SetEntry
import com.ackbox.raft.api.ReplicaNode
import com.ackbox.raft.api.ReplicaNode.Append
import com.ackbox.raft.api.ReplicaNode.Vote
import com.ackbox.raft.config.NodeConfig
import com.ackbox.raft.networking.NodeNetworking
import com.ackbox.raft.networking.NodeNetworkingChange
import com.ackbox.raft.store.KV
import com.ackbox.raft.support.Callback
import com.ackbox.raft.support.CommitIndexMismatchException
import com.ackbox.raft.support.NodeLogger
import com.ackbox.raft.support.NotLeaderException
import com.ackbox.raft.support.ReplicaStateMismatchException
import com.ackbox.raft.support.ReplyTermInvariantException
import com.ackbox.raft.support.TemporaryFile
import com.ackbox.raft.types.*
import com.ackbox.raft.types.LogItem.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import java.util.UUID
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

    private val logger: NodeLogger = NodeLogger.forNode(config.nodeId, LocalNode::class)

    override val nodeId: String = config.nodeId

    fun start() {
        val operationId = UUID.randomUUID().toString()
        config.partitions.forEach { partition ->
            val electionCallback = createElectionCallback(partition)
            val heartbeatCallback = createHeartbeatCallback(partition)
            val snapshotCallback = createSnapshotCallback(partition)
            locked.withLock(partition, START_NODE_OPERATION, operationId) { state ->
                state.start(electionCallback, heartbeatCallback, snapshotCallback)
            }
        }
    }

    fun stop() {
        val operationId = UUID.randomUUID().toString()
        config.partitions.forEach { partition ->
            locked.withLock(partition, STOP_NODE_OPERATION, operationId) { state -> state.stop() }
        }
    }

    fun describeState() {
        val operationId = UUID.randomUUID().toString()
        config.partitions.forEach { partition ->
            locked.withLock(partition, DESCRIBE_STATE_OPERATION, operationId) { state -> state.log.describe() }
        }
    }

    override fun setEntry(input: SetEntry.Input): SetEntry.Output {
        val operationId = UUID.randomUUID().toString()
        val entry = KV.fromByteArray(input.entry.array())
        val partition = Partition(entry.key.hashCode() % config.partitionCount)
        return locked.withLock(partition, REQUEST_APPEND_OPERATION, operationId) { state ->
            // Apply entries the replicated to the leader's log before doing anything.
            val consensusMetadata = state.metadata.consensusMetadata
            val lastItemIndex = state.log.getLastItemIndex()
            val leaderTerm = consensusMetadata.currentTerm
            val data = listOf(entry.value.array())
            val items = convertToLogItems(Type.STORE_CHANGE, lastItemIndex, leaderTerm, data)
            appendItem(operationId, state, items)
            return@withLock SetEntry.Output(consensusMetadata.leaderId)
        }
    }

    override fun getEntry(input: GetEntry.Input): GetEntry.Output {
        val operationId = UUID.randomUUID().toString()
        val partition = Partition(input.key.hashCode() % config.partitionCount)
        return locked.withLock(partition, REQUEST_RETRIEVE_OPERATION, operationId) { state ->
            // Check whether the current node is the leader. If not, simply fail the request letting caller know who is
            // the leader for the current term.
            val consensusMetadata = state.metadata.consensusMetadata
            val leaderId = consensusMetadata.leaderId
            if (consensusMetadata.mode != NodeMode.LEADER) {
                throw NotLeaderException(leaderId)
            }

            // Retrieve committed entry from key-value store.
            GetEntry.Output(leaderId, state.getStoreValue(input.key))
        }
    }

    override fun addNode(input: AddNode.Input): AddNode.Output {
        val partition = Partition.GLOBAL
        val snapshot = locked.withLock(partition, REQUEST_ADD_NODE_OPERATION, input.requestId) { state ->
            // Check whether the current node is the leader. If not, simply fail the request letting caller know who is
            // the leader for the current term.
            val consensusMetadata = state.metadata.consensusMetadata
            if (consensusMetadata.mode != NodeMode.LEADER) {
                throw NotLeaderException(consensusMetadata.leaderId)
            }
            return@withLock state.getLatestSnapshot()
        }

        // Try to activate the new node by sending the latest snapshot. In this process, we create a temporary remote
        // facade in order to send the latest leader's snapshot. If it's successful, the leader will commit a log item
        // that will cause the new remote to be added to the leader's networking setup.
        val metadata =
            locked.withLock(partition, REQUEST_ADD_NODE_OPERATION, input.requestId) { state -> state.metadata }
        val channel = input.address.toChannel()
        try {
            val remoteNode = RemoteNode(config, channel)
            remoteNode.sendSnapshot(input.requestId, metadata, snapshot)
        } finally {
            channel.runCatching { shutdownNow() }
        }

        // Replicate the configuration change.
        return locked.withLock(partition, REQUEST_ADD_NODE_OPERATION, input.requestId) { state ->
            val consensusMetadata = state.metadata.consensusMetadata
            val lastItemIndex = state.log.getLastItemIndex()
            val leaderTerm = consensusMetadata.currentTerm
            val change = NodeNetworkingChange(NodeNetworkingChange.Type.ADDED, input.address)
            val data = listOf(change.toByteArray())
            val items = convertToLogItems(Type.NETWORKING_CHANGE, lastItemIndex, leaderTerm, data)
            appendItem(input.requestId, state, items)
            return@withLock AddNode.Output(consensusMetadata.leaderId)
        }
    }

    override fun removeNode(input: RemoveNode.Input): RemoveNode.Output {
        // Replicate the configuration change.
        return locked.withLock(Partition.GLOBAL, REQUEST_REMOVE_NODE_OPERATION, input.requestId) { state ->
            val consensusMetadata = state.metadata.consensusMetadata
            val lastItemIndex = state.log.getLastItemIndex()
            val leaderTerm = consensusMetadata.currentTerm
            val change = NodeNetworkingChange(NodeNetworkingChange.Type.REMOVED, input.address)
            val data = listOf(change.toByteArray())
            val items = convertToLogItems(Type.NETWORKING_CHANGE, lastItemIndex, leaderTerm, data)
            appendItem(input.requestId, state, items)
            return@withLock RemoveNode.Output(consensusMetadata.leaderId)
        }
    }

    override suspend fun handleAppend(input: Append.Input): Append.Output {
        // We ensure the issuer of the request is a node that we know about.
        remotes.ensureValidRemote(input.leaderId)

        // Lock the state in order to avoid inconsistencies while checks and validations are being performed for the
        // append request.
        val partition = input.leaderPartition
        val operationId = input.requestId
        return locked.withLock(partition, HANDLE_APPEND_OPERATION, operationId) { state ->
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
            if (!state.containsItem(previousLogIndex, previousLogTerm)) {
                logger.info("Log mismatch for logIndex=[{}] and logTerm=[{}]", previousLogIndex, previousLogTerm)
                // Optimization can be done in order to ensure minimum retransmission. Instead of returning
                // previousLogIndex - 1 for the case of index mismatch, we can return state.log.getLastItemIndex().
                val lastLogIndex = state.log.getLastItemIndex()
                val correctionLogIndex =
                    if (lastLogIndex == previousLogIndex) previousLogIndex.decremented() else lastLogIndex
                throw ReplicaStateMismatchException(currentTerm, correctionLogIndex)
            }

            // The node can now safely apply the log entries to its log.
            logger.info("Append entry ready to be committed until index [{}]", input.leaderCommitIndex)
            val leaderCommitIndex = input.leaderCommitIndex
            val processedLastLogItemIndex = state.appendLogItems(input.items)
            state.commitLogItems(leaderCommitIndex)
            return@withLock Append.Output(currentTerm, processedLastLogItemIndex)
        }
    }

    override suspend fun handleVote(input: Vote.Input): Vote.Output {
        // We ensure the issuer of the request is a node that we know about.
        remotes.ensureValidRemote(input.candidateId)

        // Lock the state in order to avoid inconsistencies while checks and validations are being performed for the
        // vote request.
        val partition = input.candidatePartition
        val operationId = input.requestId
        return locked.withLock(partition, HANDLE_VOTE_OPERATION, operationId) { state ->
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
            logger.info("Vote granted to candidateId [{}]", input.candidateId)
            state.updateVote(input.candidateId)
            return@withLock Vote.Output(state.metadata.consensusMetadata.currentTerm)
        }
    }

    @Suppress("BlockingMethodInNonBlockingContext")
    override suspend fun handleSnapshot(inputs: Flow<ReplicaNode.Snapshot.Input>): ReplicaNode.Snapshot.Output {
        return TemporaryFile(Snapshot.COMPRESSED_SNAPSHOT_FILENAME).use { file ->
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
                locked.withLock(input.leaderPartition, HANDLE_SNAPSHOT_OPERATION, input.requestId) { state ->
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
            val partition = lastInput!!.leaderPartition
            val operationId = lastInput!!.requestId
            return@use locked.withLock(partition, HANDLE_SNAPSHOT_OPERATION, operationId) { state ->
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
                state.restoreSnapshot(lastIncludedLogIndex, lastIncludedLogTerm, file.toPath().parent)

                logger.info("Snapshot successfully loaded for input [{}]", lastInput)
                return@withLock ReplicaNode.Snapshot.Output(state.metadata.consensusMetadata.currentTerm)
            }
        }
    }

    private fun appendItem(operationId: String, state: PartitionState, items: List<LogItem> = emptyList()) {
        // Check whether the current node is the leader. If not, simply fail the request letting caller know who is
        // the leader for the current term.
        val consensusMetadata = state.metadata.consensusMetadata
        val leaderId = consensusMetadata.leaderId
        if (consensusMetadata.mode != NodeMode.LEADER) {
            throw NotLeaderException(leaderId)
        }

        // Apply entries the replicated to the leader's log before doing anything.
        val lastLogItemIndex = state.appendLogItems(items)

        // Dispatch requests in parallel to all replicas/remote in the cluster in order to append the new entry to
        // their logs. In these requests, we send the current leader information (id and term) as well as pack all
        // entries that we think the remote node will need to catch up with leader's log. In order to do so, we use
        // the remote#nextLogIndex and remote#matchLogIndex information.
        val remoteStates = try {
            remotes.appendItems(operationId, state.metadata, state.log, state.getLatestSnapshot())
        } catch (e: ReplyTermInvariantException) {
            // Remotes will throw term invariant exception whenever they detect that their term is greater than the
            // node that generated the append request (case of 'this' node since it's performing the append
            // request). This might mean that there are multiple nodes thinking they are leaders in the cluster.
            state.transitionToFollower(e.remoteTerm)
            throw e
        }

        // Verify replication consensus status across majority in the cluster.
        val remoteMatchLogIndexes = remoteStates.map { it.matchLogIndex }
        logger.info("Updating commit index with remote match indexes [{}]", remoteMatchLogIndexes)
        val matchIndexes = mutableListOf<Long>()
            .apply { remoteMatchLogIndexes.forEach { add(it.value) } }
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
    }

    private fun createElectionCallback(partition: Partition): Callback {
        return {
            val operationId = UUID.randomUUID().toString()
            logger.info("Starting a new election")
            // Collect votes from remotes in the cluster. If we detect a violation of the term invariant, we stop the
            // process and transition the current node to follower mode.
            val votes = locked.withLock(partition, REQUEST_VOTE_OPERATION, operationId) { state ->
                state.transitionToCandidate()
                try {
                    remotes.requestVote(operationId, state.metadata, state.log)
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
            locked.withLock(partition, REQUEST_VOTE_OPERATION, operationId) { state ->
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

    private fun createHeartbeatCallback(partition: Partition): Callback {
        return {
            try {
                val operationId = UUID.randomUUID().toString()
                logger.info("Started heartbeat with followers")
                locked.withLock(partition, HEARTBEAT_OPERATION, operationId) { state -> appendItem(operationId, state) }
                logger.debug("Finished heartbeat with followers")
            } catch (e: Exception) {
                logger.error("Error while executing heartbeat", e)
            }
        }
    }

    private fun createSnapshotCallback(partition: Partition): Callback {
        return {
            try {
                val operationId = UUID.randomUUID().toString()
                logger.info("Taking a new snapshot of node's state")
                val snapshot = locked.withLock(partition, REQUEST_SNAPSHOT_OPERATION, operationId) { state ->
                    state.takeSnapshot()
                    state.getLatestSnapshot()
                }
                logger.info("Finished taking snapshot [{}]", snapshot)
            } catch (e: Exception) {
                logger.error("Error while executing snapshot", e)
            }
        }
    }

    private fun convertToLogItems(type: Type, lastItemIndex: Index, term: Term, data: List<ByteArray>): List<LogItem> {
        return data.mapIndexed { index, bytes ->
            val newItemIndex = lastItemIndex.incrementedBy(index.toLong() + 1)
            LogItem(type, newItemIndex, term, bytes)
        }
    }

    companion object {

        fun fromConfig(config: NodeConfig, networking: NodeNetworking): LocalNode {
            val state = LocalNodeState.fromConfig(config, networking)
            val remotes = RemoteNodes.fromConfig(config, networking.getChannels())
            networking.setOnChannelAdded { channel -> remotes.activateNode(channel) }
            networking.setOnChannelRemoved { channel -> remotes.deactivateNode(channel) }
            return LocalNode(config, state, remotes)
        }
    }
}
