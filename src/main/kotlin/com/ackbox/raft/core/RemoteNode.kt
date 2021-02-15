package com.ackbox.raft.core

import com.ackbox.raft.api.AppendReply
import com.ackbox.raft.api.AppendRequest
import com.ackbox.raft.api.PrivateNodeGrpcKt.PrivateNodeCoroutineStub
import com.ackbox.raft.api.SnapshotRequest
import com.ackbox.raft.api.VoteReply
import com.ackbox.raft.api.VoteRequest
import com.ackbox.raft.config.NodeConfig
import com.ackbox.raft.log.LogItem
import com.ackbox.raft.log.ReplicatedLog
import com.ackbox.raft.networking.NamedChannel
import com.ackbox.raft.state.Metadata
import com.ackbox.raft.state.RemoteNodeState
import com.ackbox.raft.statemachine.Snapshot
import com.ackbox.raft.support.NodeLogger
import com.ackbox.raft.support.ReplyTermInvariantException
import com.google.protobuf.ByteString
import io.grpc.StatusException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.time.Clock
import java.util.concurrent.atomic.AtomicReference

class RemoteNode(private val config: NodeConfig, private val channel: NamedChannel, private val clock: Clock) {

    private val logger: NodeLogger = NodeLogger.from(config.nodeId, RemoteNode::class)
    private val remoteClient: PrivateNodeCoroutineStub = PrivateNodeCoroutineStub(channel)
    private val remoteState: AtomicReference<RemoteNodeState> = AtomicReference(RemoteNodeState())

    fun sendAppend(metadata: Metadata, log: ReplicatedLog, snapshot: Snapshot): RemoteNodeState {
        // Update internal representation of the follower node according to the reply. Lock on the
        // state and return a consistent snapshot updated according to the response from the peer.
        val leaderTerm = metadata.currentTerm
        return remoteState.updateAndGet { state ->
            // If the follower node needs an entry that is no longer present in the logs of the leader
            // (meaning that the leader trimmed its log beyond [state.nextLogIndex]), the leader will
            // send a snapshot to the follower. This operation replaces the append entry.
            if (state.nextLogIndex < log.getFirstItemIndex()) {
                logger.info("Sending snapshot to remote=[{}] instead", channel.id)
                sendSnapshotAsync(snapshot)
                return@updateAndGet state
            }

            // Assemble all required log items that need to be replicated to peers in the cluster.
            val items = retrieveItems(state, log)

            // Retrieve metadata about the log item corresponding to the last last log item present
            // in the follower node.
            val previousLogItem = log.getItem(state.nextLogIndex - 1)
            val previousLogIndex = previousLogItem?.index ?: snapshot.lastIncludedLogIndex
            val previousLogTerm = previousLogItem?.term ?: snapshot.lastIncludedLogTerm

            // Send the append request to the follower node.
            logger.info("Sending append entries to remote=[{}]", channel.id)
            val reply = try {
                runBlocking {
                    withTimeout(config.remoteRpcTimeoutDuration.toMillis()) {
                        val request = createAppendRequest(metadata, previousLogIndex, previousLogTerm, items)
                        remoteClient.handleAppend(request)
                    }
                }
            } catch (e: StatusException) {
                logger.warn("Error while contacting remote=[{}] and status=[{}]", channel.id, e.status.code, e)
                return@updateAndGet state
            }

            // Check whether the follower node and this node agree around leadership.
            if (leaderTerm < reply.currentTerm) {
                throw ReplyTermInvariantException(leaderTerm, reply.currentTerm)
            }

            return@updateAndGet if (reply.status == AppendReply.Status.SUCCESS) {
                // If the response from the follower is success, it means that the follower's log is caught up
                // with leader's. It is safe to count the follower as successful entry replication.
                logger.info("Remote [{}] is successfully caught up", channel.id)
                state.copy(nextLogIndex = reply.lastLogIndex + 1, matchLogIndex = reply.lastLogIndex)
            } else {
                // If the response from the follower is NOT success, it means that the follower's log is caught up
                // with leader's. We update the follower state according to its response in an attempt to
                // fix log inconsistencies or missing entries.
                logger.info("Remote [{}] is needs to catch up with leader", channel.id)
                state.copy(nextLogIndex = reply.lastLogIndex + 1)
            }
        }
    }

    fun sendVote(metadata: Metadata, log: ReplicatedLog): Boolean {
        // Initiate voting procedure with follower node.
        val candidateTerm = metadata.currentTerm
        val lastItem = log.getItem(log.getLastItemIndex())

        // Send vote request to follower node.
        val reply = try {
            runBlocking {
                return@runBlocking withTimeout(config.remoteRpcTimeoutDuration.toMillis()) {
                    remoteClient.handleVote(createVoteRequest(metadata, candidateTerm, lastItem))
                }
            }
        } catch (e: StatusException) {
            logger.warn("Error while contacting remote=[{}] and status=[{}]", channel.id, e.status.code, e)
            return false
        }

        // Check whether the follower node and this node agree around leadership.
        if (candidateTerm < reply.currentTerm) {
            throw ReplyTermInvariantException(candidateTerm, reply.currentTerm)
        }

        // If terms are compatible, return whether the node voted for the candidate.
        return reply.status == VoteReply.Status.VOTE_GRANTED
    }

    fun resetState(nextLogIndex: Long) {
        remoteState.updateAndGet { state -> state.copy(nextLogIndex = nextLogIndex) }
    }

    private fun sendSnapshotAsync(snapshot: Snapshot) {
        // Asynchronously collect and send snapshot to follower. This operation is
        // non-blocking and executed in a different scope. The code will return
        // the current remote state so that the leader can continue processing.
        // The behavior here follows a fire-and-forget semantics.
        GlobalScope.launch(Dispatchers.IO) {
            logger.info("Sending snapshot to remote [{}]", channel.id)
            val snapshotFile = snapshot.dataPath.toFile()
            if (!snapshotFile.exists()) {
                logger.warn("Skipping snapshot to remote [{}] since no data file was found", channel.id)
                return@launch
            }
            var size: Int
            val buffer = ByteArray(BUFFER_SIZE_IN_BYTES)
            val request = flow<SnapshotRequest> {
                snapshotFile.inputStream().use { input ->
                    while (input.read(buffer).also { size = it } > 0) {
                        emit(createSnapshotRequest(buffer, size))
                    }
                }
            }
            remoteClient.handleSnapshot(request)
        }.invokeOnCompletion { cause ->
            if (cause != null) {
                logger.warn("Error while sending snapshot to remote [{}]", channel.id, cause)
            } else {
                logger.info("Finished sending snapshot to remote [{}]", channel.id)
            }
        }
    }

    private fun createAppendRequest(
        metadata: Metadata,
        previousLogIndex: Long,
        previousLogTerm: Long,
        items: List<LogItem>
    ): AppendRequest {
        return AppendRequest.newBuilder()
            .setTimestamp(clock.millis())
            .setLeaderId(metadata.leaderId)
            .setLeaderTerm(metadata.currentTerm)
            .setLeaderCommitIndex(metadata.commitIndex)
            .setPreviousLogIndex(previousLogIndex)
            .setPreviousLogTerm(previousLogTerm)
            .addAllEntries(items.map { it.toEntry() })
            .build()
    }

    private fun createVoteRequest(metadata: Metadata, candidateTerm: Long, lastItem: LogItem?): VoteRequest {
        return VoteRequest.newBuilder()
            .setTimestamp(clock.millis())
            .setCandidateId(metadata.nodeId)
            .setCandidateTerm(candidateTerm)
            .setLastLogTerm(lastItem?.term ?: UNDEFINED_ID)
            .setLastLogIndex(lastItem?.index ?: UNDEFINED_ID)
            .build()
    }

    private fun createSnapshotRequest(buffer: ByteArray, size: Int) = SnapshotRequest.newBuilder()
        .setTimestamp(clock.millis())
        .setData(ByteString.copyFrom(buffer, 0, size))
        .build()

    private fun retrieveItems(snapshot: RemoteNodeState, log: ReplicatedLog): List<LogItem> {
        val nextLogIndex = snapshot.nextLogIndex
        return (nextLogIndex..log.getLastItemIndex()).mapNotNull { log.getItem(it) }
    }

    private fun LogItem.toEntry(): AppendRequest.Entry {
        return AppendRequest.Entry.newBuilder()
            .setIndex(index)
            .setTerm(term)
            .setEntry(ByteString.copyFrom(value))
            .build()
    }

    companion object {

        private const val BUFFER_SIZE_IN_BYTES = 4096
    }
}
