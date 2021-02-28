package com.ackbox.raft.core

import com.ackbox.raft.api.AppendReply
import com.ackbox.raft.api.AppendRequest
import com.ackbox.raft.api.InternalNodeGrpcKt.InternalNodeCoroutineStub
import com.ackbox.raft.api.SnapshotRequest
import com.ackbox.raft.api.VoteReply
import com.ackbox.raft.api.VoteRequest
import com.ackbox.raft.config.NodeConfig
import com.ackbox.raft.log.ReplicatedLog
import com.ackbox.raft.networking.NamedChannel
import com.ackbox.raft.support.NodeLogger
import com.ackbox.raft.support.ReplyTermInvariantException
import com.ackbox.raft.types.Index
import com.ackbox.raft.types.LogItem
import com.ackbox.raft.types.Metadata
import com.ackbox.raft.types.Partition
import com.ackbox.raft.types.Term
import com.ackbox.raft.types.UNDEFINED_ID
import com.google.protobuf.ByteString
import io.grpc.StatusException
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.util.concurrent.atomic.AtomicReference
import javax.annotation.concurrent.ThreadSafe

@ThreadSafe
class RemoteNode(private val config: NodeConfig, val channel: NamedChannel) {

    private val logger: NodeLogger = NodeLogger.forNode("${config.nodeId}->${channel.id}", RemoteNode::class)
    private val remoteClient: InternalNodeCoroutineStub = InternalNodeCoroutineStub(channel)
    private val remoteState: AtomicReference<RemoteNodeState> = AtomicReference(RemoteNodeState())

    fun sendAppend(requestId: String, metadata: Metadata, log: ReplicatedLog, snapshot: Snapshot): RemoteNodeState {
        // Update internal representation of the follower node according to the reply. Lock on the
        // state and return a consistent snapshot updated according to the response from the peer.
        val partition = metadata.partition
        val leaderTerm = metadata.consensusMetadata.currentTerm
        return remoteState.updateAndGet { state ->
            // If the follower node needs an entry that is no longer present in the logs of the leader
            // (meaning that the leader trimmed its log beyond [state.nextLogIndex]), the leader will
            // send a snapshot to the follower. This operation replaces the append entry.
            if (log.getFirstItemIndex() >= state.nextLogIndex) {
                info(partition, "Sending snapshot [{}] to remote [{}] instead", snapshot, channel.id)
                try {
                    sendSnapshot(requestId, metadata, snapshot)
                    val nextLogIndex = snapshot.lastIncludedLogIndex.incremented()
                    val matchLogIndex = snapshot.lastIncludedLogIndex
                    return@updateAndGet state.copy(nextLogIndex = nextLogIndex, matchLogIndex = matchLogIndex)
                } catch (e: StatusException) {
                    val code = e.status.code
                    warn(partition, "Error while contacting remote=[{}] and status=[{}]", channel.id, code, e)
                    return@updateAndGet state
                }
            }

            // Assemble all required log items that need to be replicated to peers in the cluster.
            val items = retrieveItems(state, log)

            // Retrieve metadata about the log item corresponding to the last last log item present
            // in the follower node.
            val previousLogItem = log.getItem(state.nextLogIndex.decremented())!!
            val previousLogIndex = previousLogItem.index
            val previousLogTerm = previousLogItem.term

            // Send the append request to the follower node.
            val startIndex = previousLogIndex.incremented()
            val endIndex = startIndex.incrementedBy(items.size.toLong())
            info(partition, "Sending append entries [{}::{}] to remote=[{}]", startIndex, endIndex, channel.id)
            val reply = try {
                runBlocking {
                    return@runBlocking withTimeout(config.remoteRpcTimeoutDuration.toMillis()) {
                        val request = createAppendRequest(requestId, metadata, previousLogIndex, previousLogTerm, items)
                        remoteClient.handleAppend(request)
                    }
                }
            } catch (e: StatusException) {
                val code = e.status.code
                warn(partition, "Error while contacting remote=[{}] and status=[{}]", channel.id, code, e)
                return@updateAndGet state
            }

            // Check whether the follower node and this node agree around leadership.
            val replyTerm = Term(reply.currentTerm)
            if (leaderTerm < replyTerm) {
                throw ReplyTermInvariantException(leaderTerm, replyTerm)
            }

            val lastLogIndex = reply.lastLogIndex
            return@updateAndGet if (reply.status == AppendReply.Status.SUCCESS) {
                // If the response from the follower is success, it means that the follower's log is caught up
                // with leader's. It is safe to count the follower as successful entry replication.
                info(partition, "Remote [{}] is successfully caught up: lastLogIndex=[{}]", channel.id, lastLogIndex)
                state.copy(nextLogIndex = Index(lastLogIndex + 1), matchLogIndex = Index(lastLogIndex))
            } else {
                // If the response from the follower is NOT success, it means that the follower's log is caught up
                // with leader's. We update the follower state according to its response in an attempt to
                // fix log inconsistencies or missing entries.
                info(partition, "Remote [{}] is behind: lastLogIndex=[{}]", channel.id, lastLogIndex)
                state.copy(nextLogIndex = Index(lastLogIndex + 1))
            }
        }
    }

    fun sendVote(requestId: String, metadata: Metadata, log: ReplicatedLog): Boolean {
        // Initiate voting procedure with follower node.
        val partition = metadata.partition
        val candidateTerm = metadata.consensusMetadata.currentTerm
        val lastItem = log.getItem(log.getLastItemIndex())

        // Send vote request to follower node.
        val reply = try {
            runBlocking {
                return@runBlocking withTimeout(config.remoteRpcTimeoutDuration.toMillis()) {
                    remoteClient.handleVote(createVoteRequest(requestId, metadata, candidateTerm, lastItem))
                }
            }
        } catch (e: StatusException) {
            val code = e.status.code
            warn(partition, "Error while contacting remote=[{}] and status=[{}]", channel.id, code, e)
            return false
        }

        // Check whether the follower node and this node agree around leadership.
        val replyTerm = Term(reply.currentTerm)
        if (candidateTerm < replyTerm) {
            throw ReplyTermInvariantException(candidateTerm, replyTerm)
        }

        // If terms are compatible, return whether the node voted for the candidate.
        return reply.status == VoteReply.Status.VOTE_GRANTED
    }

    fun sendSnapshot(requestId: String, metadata: Metadata, snapshot: Snapshot) {
        // Do not use RPC timeout since snapshot transfers can be arbitrary long.
        runBlocking { sendSnapshotAsync(requestId, metadata, snapshot) }
    }

    fun resetState(nextLogIndex: Index) {
        remoteState.updateAndGet { state -> state.copy(nextLogIndex = nextLogIndex) }
    }

    override fun toString(): String = "${RemoteNode::class.simpleName}(id=${config.nodeId}, channel=${channel.address})"

    @Suppress("BlockingMethodInNonBlockingContext")
    private suspend fun sendSnapshotAsync(requestId: String, metadata: Metadata, snapshot: Snapshot) {
        val partition = metadata.partition
        info(partition, "Sending snapshot to remote [{}]", channel.id)
        val snapshotFile = snapshot.compressedFilePath.toFile()
        if (!snapshotFile.exists()) {
            warn(partition, "Skipping snapshot to remote [{}] since no data file was found", channel.id)
            return
        }
        var size: Int
        val buffer = ByteArray(BUFFER_SIZE_IN_BYTES)
        val request = flow {
            snapshotFile.inputStream().use { input ->
                while (input.read(buffer).also { size = it } > 0) {
                    emit(createSnapshotRequest(requestId, metadata, snapshot, buffer, size))
                }
            }
        }
        remoteClient.handleSnapshot(request)
    }

    private fun createAppendRequest(
        requestId: String,
        metadata: Metadata,
        previousLogIndex: Index,
        previousLogTerm: Term,
        items: List<LogItem>
    ): AppendRequest {
        return AppendRequest.newBuilder().apply {
            this.timestamp = config.clock.millis()
            this.requestId = requestId
            this.leaderPartition = metadata.partition.value
            metadata.consensusMetadata.leaderId?.let { this.leaderId = it }
            this.leaderTerm = metadata.consensusMetadata.currentTerm.value
            this.leaderCommitIndex = metadata.commitMetadata.commitIndex.value
            this.previousLogIndex = previousLogIndex.value
            this.previousLogTerm = previousLogTerm.value
            this.addAllEntries(items.map { it.toEntry() })
        }.build()
    }

    private fun createVoteRequest(
        requestId: String,
        metadata: Metadata,
        candidateTerm: Term,
        lastItem: LogItem?
    ): VoteRequest {
        return VoteRequest.newBuilder().apply {
            this.timestamp = config.clock.millis()
            this.requestId = requestId
            this.candidatePartition = metadata.partition.value
            this.candidateId = metadata.nodeId
            this.candidateTerm = candidateTerm.value
            this.lastLogTerm = lastItem?.term?.value ?: UNDEFINED_ID
            this.lastLogIndex = lastItem?.index?.value ?: UNDEFINED_ID
        }.build()
    }

    private fun createSnapshotRequest(
        requestId: String,
        metadata: Metadata,
        snapshot: Snapshot,
        buffer: ByteArray,
        size: Int
    ): SnapshotRequest {
        return SnapshotRequest.newBuilder().apply {
            this.timestamp = config.clock.millis()
            metadata.consensusMetadata.leaderId?.let { this.leaderId = it }
            this.leaderPartition = metadata.partition.value
            this.requestId = requestId
            this.leaderTerm = metadata.consensusMetadata.currentTerm.value
            this.lastIncludedLogTerm = snapshot.lastIncludedLogTerm.value
            this.lastIncludedLogIndex = snapshot.lastIncludedLogIndex.value
            this.data = ByteString.copyFrom(buffer, 0, size)
        }.build()
    }

    private fun retrieveItems(snapshot: RemoteNodeState, log: ReplicatedLog): List<LogItem> {
        val nextLogIndex = snapshot.nextLogIndex
        return (nextLogIndex.value..log.getLastItemIndex().value).mapNotNull { log.getItem(Index(it)) }
    }

    private fun LogItem.toEntry(): AppendRequest.Entry {
        return AppendRequest.Entry.newBuilder().apply {
            this@apply.index = this@toEntry.index.value
            this@apply.term = this@toEntry.term.value
            this@apply.entry = ByteString.copyFrom(value)
        }.build()
    }

    private fun info(partition: Partition, pattern: String, vararg args: Any) {
        logger.info("[partition=${partition.value}] $pattern", *args)
    }

    private fun warn(partition: Partition, pattern: String, vararg args: Any) {
        logger.warn("[partition=${partition.value}] $pattern", *args)
    }

    companion object {

        private const val BUFFER_SIZE_IN_BYTES = 4096
    }
}
