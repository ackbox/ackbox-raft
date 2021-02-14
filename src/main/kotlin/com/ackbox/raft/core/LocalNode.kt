package com.ackbox.raft.core

import com.ackbox.raft.config.NodeConfig
import com.ackbox.raft.core.LeaderNode.Get
import com.ackbox.raft.core.LeaderNode.Set
import com.ackbox.raft.core.ReplicaNode.Append
import com.ackbox.raft.core.ReplicaNode.Vote
import com.ackbox.raft.state.Callback
import com.ackbox.raft.state.LocalNodeState
import com.ackbox.raft.state.Metadata
import com.ackbox.raft.log.ReplicatedLog.LogItem
import com.ackbox.raft.support.CommitIndexMismatchException
import com.ackbox.raft.support.NodeLogger
import com.ackbox.raft.support.NotLeaderException
import com.ackbox.raft.support.ReplicaStateMismatchException
import com.ackbox.raft.support.ReplyTermInvariantException
import java.nio.ByteBuffer

/**
 * Raft node API implementation. The behavior implemented here follows that paragraphs $5.1, $5.2, $5.3 and $5.4
 * of the original paper. The name of the variables may have been slightly modified for better clarity. However,
 * most of the logic follows closely the proposed algorithm in the paper.
 *
 * @see [Raft Paper](https://raft.github.io/raft.pdf)
 */
class LocalNode(private val locked: LocalNodeState, private val remotes: RemoteNodes) : LeaderNode, ReplicaNode {

    private val logger: NodeLogger = NodeLogger.from(locked.nodeId, LocalNode::class)

    override val nodeId: String = locked.nodeId

    fun start() {
        val electionCallback = createElectionCallback()
        val heartbeatCallback = createHeartbeatCallback()
        locked.withLock(START_NODE_OPERATION) { state -> state.start(electionCallback, heartbeatCallback) }
    }

    fun stop() {
        locked.withLock(STOP_NODE_OPERATION) { state -> state.stop() }
    }

    fun describeState() {
        locked.withLock(DESCRIBE_STATE_OPERATION) { state ->
            state.getLog().describe()
        }
    }

    override fun setItem(input: Set.Input): Set.Output {
        return locked.withLock(REQUEST_APPEND_OPERATION) { state ->
            // Check whether the current node is the leader. If not, simply fail the request letting
            // caller know who is the leader for the current term.
            val leaderId = state.getCurrentLeaderId()
            if (state.getCurrentMode() != Metadata.NodeMode.LEADER) {
                throw NotLeaderException(leaderId)
            }

            // Apply entries the replicated to the leader's log before doing anything.
            val log = state.getLog()
            val leaderTerm = state.getCurrentTerm()
            val items = convertToLogItems(leaderTerm, log.getLastItemIndex(), input.data)
            val lastLogItemIndex = state.appendLogItems(items)

            // Dispatch requests in parallel to all peers in the cluster in order to append
            // the new entry to their logs. In these requests, we send the current leader
            // information (id and term) as well as pack all entries that we think the peer
            // node will need to catch up with leader's log. In order to do so, we use the
            // peer#nextLogIndex and peer#matchLogIndex information.
            val peerStates = try {
                remotes.appendItems(state.getMetadata(), log)
            } catch (e: ReplyTermInvariantException) {
                // Peers will throw term invariant exception whenever they detect that their
                // term is greater than the node that generated the append request (case of
                // 'this' node since it's performing the append request). This might mean that
                // there are multiple nodes thinking they are leaders in the cluster.
                state.transitionToFollower(e.remoteTerm)
                throw e
            }

            // Verify replication consensus status across majority in the cluster.
            val matchIndexes = mutableListOf<Long>()
                .apply { peerStates.forEach { add(it.matchLogIndex) } }
                .apply { add(lastLogItemIndex) }
                .sorted()
            val commitIndex = matchIndexes[remotes.size() / 2]
            logger.info("Commit index updated from [{}] to [{}]", state.getCommitIndex(), commitIndex)

            // Only update the commit index if there's consensus in the replication results
            // from all the peers in the cluster.
            if (commitIndex < state.getCommitIndex()) {
                throw CommitIndexMismatchException(leaderId, state.getCommitIndex(), commitIndex)
            }

            // Commit items that were confirmed to be replicated across the cluster.
            state.commitLogItems(commitIndex)
            return@withLock Set.Output(leaderId, lastLogItemIndex)
        }
    }

    override fun getItem(input: Get.Input): Get.Output {
        return locked.withLock(REQUEST_RETRIEVE_OPERATION) { state ->
            // Check whether the current node is the leader. If not, simply fail the request letting
            // caller know who is the leader for the current term.
            val leaderId = state.getCurrentLeaderId()
            if (state.getCurrentMode() != Metadata.NodeMode.LEADER) {
                throw NotLeaderException(leaderId)
            }

            // Retrieve committed entry from state machine.
            Get.Output(state.getCurrentLeaderId(), state.getCommittedLogItem(input.itemSqn))
        }
    }

    override fun handleAppend(input: Append.Input): Append.Output {
        // Lock the state in order to avoid inconsistencies while checks and validations
        // are being performed for the append request.
        return locked.withLock(HANDLE_APPEND_OPERATION) { state ->
            // Check whether there are multiple leaders sending append requests. If multiple leaders
            // are detected, force them to step down and restart an election by incrementing the term.
            val leaderId = input.leaderId
            val leaderTerm = input.leaderTerm
            val currentTerm = state.getCurrentTerm()

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
            // We may have noticed that we do not try to check the logs are exactly in sync. The only
            // necessary guarantee is that the node's and leader's log agree at the point previousLogIndex.
            // We do not care if the node's log is ahead. We just make sure that node's log is not behind.
            // The leader will then made the necessary adjustments in order to fix inconsistencies. For
            // instance, entries will be retransmitted until the logs are synchronized.
            val previousLogIndex = input.previousLogIndex
            val previousLogTerm = input.previousLogTerm
            if (!state.getLog().containsItem(previousLogIndex, previousLogTerm)) {
                logger.info("Log mismatch for logIndex=[{}] and logTerm=[{}]", previousLogIndex, previousLogTerm)
                // Optimization can be done in order to ensure minimum retransmission. Instead of returning
                // previousLogIndex - 1 for the case of index mismatch, we can return state.getLog().getLastItemIndex().
                val lastLogItem = state.getLog().getItem(state.getLog().getLastItemIndex())!!
                val lastLogIndex = lastLogItem.index
                val correctionLogIndex = if (lastLogIndex == previousLogIndex) previousLogIndex - 1 else lastLogIndex
                throw ReplicaStateMismatchException(currentTerm, correctionLogIndex)
            }

            // The node can now safely apply the log entries to its log.
            logger.info("Append entry ready to be replicated")
            val leaderCommitIndex = input.leaderCommitIndex
            val processedLastLogItemIndex = state.appendLogItems(input.items)
            state.commitLogItems(leaderCommitIndex)
            return@withLock Append.Output(currentTerm, processedLastLogItemIndex)
        }
    }

    override fun handleVote(input: Vote.Input): Vote.Output {
        // Lock the state in order to avoid inconsistencies while checks and validations
        // are being performed for the vote request.
        return locked.withLock(HANDLE_VOTE_OPERATION) { state ->
            // We ensure a valid candidate by checking the following cases:
            //   1. Check whether the candidate node has a term greater than the term known by this node.
            //   2. Check whether the node can vote for the candidate.
            //   3. Check whether the candidate node's log is caught up with the current node's log.
            state.ensureValidCandidate(input.candidateId, input.candidateTerm, input.lastLogIndex, input.lastLogTerm)

            // In order to grant a vote, two conditions should be met:
            //   1. No vote has been issued or vote issued == candidateId.
            //   2. Candidate's log is caught up with node's.
            // Note that the log consistency restriction allows for a uni-directional flow for
            // entries: entries flow from leader to follower always. A leader will never be elected
            // if its log is behind.
            logger.info("Vote granted to candidateId=[{}]", input.candidateId)
            state.updateVote(input.candidateId)
            return@withLock Vote.Output(state.getCurrentTerm())
        }
    }

    private fun createElectionCallback(): Callback {
        return {
            logger.info("Starting a new election")
            // Collect votes from peers in the cluster. If we detect a violation of the term
            // invariant, we stop the process and transition the current node to follower mode.
            val votes = locked.withLock(REQUEST_VOTE_OPERATION) { state ->
                state.transitionToCandidate()
                try {
                    remotes.requestVote(state.getMetadata(), state.getLog())
                } catch (e: ReplyTermInvariantException) {
                    state.transitionToFollower(e.remoteTerm)
                    throw e
                }
            }
            // Check whether candidate node won the election. Here we add '1' to total
            // to take into account the vote of the current node, which of course is
            // voting for itself.
            locked.withLock(REQUEST_VOTE_OPERATION) { state ->
                val total = 1 + votes.map { if (it) 1 else 0 }.sum()
                if (total > remotes.size() / 2) {
                    logger.info("Node has been elected as leader")
                    state.transitionToLeader()
                } else {
                    logger.info("Node is transitioning to follower due to not enough votes")
                    state.transitionToFollower(state.getCurrentTerm())
                }
            }
        }
    }

    private fun createHeartbeatCallback(): Callback {
        return {
            try {
                logger.debug("Started heartbeat with")
                val output = setItem(Set.Input(emptyList()))
                logger.debug("Finished heartbeat with itemIndex=[{}]", output.itemSqn)
            } catch (e: Exception) {
                logger.error("Error while executing heartbeat", e)
            }
        }
    }

    private fun convertToLogItems(currentTerm: Long, lastItemIndex: Long, data: List<ByteArray>): List<LogItem> {
        var entryIndex = lastItemIndex + 1
        return data.map { LogItem(entryIndex++, currentTerm, it) }
    }

    companion object {

        fun fromConfig(config: NodeConfig, peers: RemoteNodes): LocalNode {
            return LocalNode(LocalNodeState.fromConfig(config), peers)
        }
    }
}
