package com.ackbox.raft.core

import com.ackbox.raft.AppendReply
import com.ackbox.raft.AppendRequest
import com.ackbox.raft.PrivateNodeGrpc
import com.ackbox.raft.PrivateNodeGrpc.PrivateNodeBlockingStub
import com.ackbox.raft.VoteReply
import com.ackbox.raft.VoteRequest
import com.ackbox.raft.networking.NamedChannel
import com.ackbox.raft.state.Metadata
import com.ackbox.raft.state.RemoteNodeState
import com.ackbox.raft.log.ReplicatedLog
import com.ackbox.raft.log.ReplicatedLog.LogItem
import com.ackbox.raft.support.NodeLogger
import com.ackbox.raft.support.ReplyTermInvariantException
import com.google.protobuf.ByteString
import io.grpc.StatusRuntimeException
import java.lang.IllegalStateException
import java.time.Clock
import java.util.concurrent.atomic.AtomicReference

class RemoteNode(localNodeId: String, private val channel: NamedChannel, private val clock: Clock) {

    private val logger: NodeLogger = NodeLogger.from(localNodeId, RemoteNode::class)
    private val remoteClient: PrivateNodeBlockingStub = PrivateNodeGrpc.newBlockingStub(channel)
    private val remoteState: AtomicReference<RemoteNodeState> = AtomicReference(RemoteNodeState())

    fun appendItems(metadata: Metadata, log: ReplicatedLog): RemoteNodeState {
        // Update internal representation of the peer node according to the reply. Lock on the
        // state and return a consistent snapshot updated according to the response from the peer.
        val leaderTerm = metadata.getCurrentTerm()
        return remoteState.updateAndGet { state ->
            // Assemble all required log items that need to be replicated to peers in the cluster.
            val previousItem = log.getItem(state.nextLogIndex - 1)!!
            val items = retrieveItems(state, log)

            // Send the append request to the peer node.
            logger.info("Sending append entries to peer=[{}]", channel.id)
            val reply = try {
                remoteClient.append(createAppendRequest(metadata, previousItem, items))
            } catch (e: StatusRuntimeException) {
                logger.warn("Error while contacting remote", e)
                return@updateAndGet state
            }

            // Check whether the peer node and this node agree around leadership.
            if (leaderTerm < reply.currentTerm) {
                throw ReplyTermInvariantException(leaderTerm, reply.currentTerm)
            }

            if (reply.status == AppendReply.Status.SUCCESS) {
                // If the response from the peer is success, it means that the peer's log is caught up
                // with leader's. It is safe to count the peer as successful entry replication.
                logger.info("Peer [{}] is successfully caught up", channel.id)
                state.copy(nextLogIndex = reply.lastLogIndex + 1, matchLogIndex = reply.lastLogIndex)
            } else {
                // If the response from the peer is NOT success, it means that the peer's log is caught up
                // with leader's. We update the peer state according to its response in an attempt to
                // fix log inconsistencies or missing entries.
                logger.info("Peer [{}] is needs to catch up with leader", channel.id)
                state.copy(nextLogIndex = reply.lastLogIndex + 1)
            }
        }
    }

    fun requestVote(metadata: Metadata, log: ReplicatedLog): Boolean {
        // Initiate voting procedure with peer node.
        val candidateTerm = metadata.getCurrentTerm()
        val lastItemIndex = log.getLastItemIndex()
        val lastItem = log.getItem(lastItemIndex) ?: throw IllegalStateException("No item found at [$lastItemIndex]")

        // Send vote request to peer node.
        val reply = try {
            remoteClient.vote(createVoteRequest(metadata, candidateTerm, lastItem))
        } catch (e: StatusRuntimeException) {
            logger.warn("Error while contacting remote", e)
            return false
        }

        // Check whether the peer node and this node agree around leadership.
        if (candidateTerm < reply.currentTerm) {
            throw ReplyTermInvariantException(candidateTerm, reply.currentTerm)
        }

        // If terms are compatible, return whether the node voted for the candidate.
        return reply.status == VoteReply.Status.VOTE_GRANTED
    }

    private fun createAppendRequest(metadata: Metadata, previousItem: LogItem, items: List<LogItem>): AppendRequest {
        return AppendRequest.newBuilder()
            .setTimestamp(clock.millis())
            .setLeaderId(metadata.getLeaderId())
            .setLeaderTerm(metadata.getCurrentTerm())
            .setPreviousLogIndex(previousItem.index)
            .setPreviousLogTerm(previousItem.term)
            .addAllEntries(items.map { it.toEntry() })
            .build()
    }

    private fun createVoteRequest(metadata: Metadata, candidateTerm: Long, lastItem: LogItem): VoteRequest {
        return VoteRequest.newBuilder()
            .setTimestamp(clock.millis())
            .setCandidateId(metadata.nodeId)
            .setCandidateTerm(candidateTerm)
            .setLastLogTerm(lastItem.term)
            .setLastLogIndex(lastItem.index)
            .build()
    }

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
}
