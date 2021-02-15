package com.ackbox.raft.core

import com.ackbox.raft.log.LogItem
import kotlinx.coroutines.flow.Flow
import java.nio.ByteBuffer

/**
 * Interface for nodes acting as replicas in the cluster.
 */
interface ReplicaNode {

    /**
     * Unique identifier of this node.
     */
    val nodeId: String

    object Append {
        data class Input(
            val leaderId: String,
            val leaderTerm: Long,
            val previousLogIndex: Long,
            val previousLogTerm: Long,
            val leaderCommitIndex: Long,
            val items: List<LogItem>
        )

        data class Output(val currentTerm: Long, val lastLogItemIndex: Long)
    }

    /**
     * Perform replication of an item. Only replicas are supposed to receive this operation.
     */
    suspend fun handleAppend(input: Append.Input): Append.Output

    object Vote {
        data class Input(
            val candidateId: String,
            val candidateTerm: Long,
            val lastLogIndex: Long,
            val lastLogTerm: Long
        )

        data class Output(val currentTerm: Long)
    }

    /**
     * Handle a vote request during a leader election. Only replicas are supposed to receive this operation.
     */
    suspend fun handleVote(input: Vote.Input): Vote.Output

    object Snapshot {
        data class Input(
            val leaderId: String,
            val leaderTerm: Long,
            val lastIncludedLogIndex: Long,
            val lastIncludedLogTerm: Long,
            val partial: ByteBuffer
        )

        data class Output(val currentTerm: Long)
    }

    /**
     * Handle a snapshot request in order to speed up the catch up process of replicas. Only replicas are
     * supposed to receive this operation.
     */
    suspend fun handleSnapshot(inputs: Flow<Snapshot.Input>): Snapshot.Output
}
