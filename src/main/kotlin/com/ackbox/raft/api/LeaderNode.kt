package com.ackbox.raft.api

import com.ackbox.raft.networking.NodeAddress
import com.ackbox.raft.store.KV
import java.nio.ByteBuffer

/**
 * Interface for nodes acting as leader in the cluster.
 */
interface LeaderNode {

    /**
     * Unique identifier of this node.
     */
    val nodeId: String

    object SetEntry {
        data class Input(val entry: ByteBuffer)
        data class Output(val leaderId: String?)
    }

    /**
     * Persist and replicate an entry to data store. Only leaders can perform this operation.
     */
    fun setEntry(input: SetEntry.Input): SetEntry.Output

    object GetEntry {
        data class Input(val key: String)
        data class Output(val leaderId: String?, val entry: ByteBuffer?)
    }

    /**
     * Get an entry from data store. Only leaders can perform this operation.
     */
    fun getEntry(input: GetEntry.Input): GetEntry.Output

    object AddNode {
        data class Input(val requestId: String, val address: NodeAddress)
        data class Output(val leaderId: String?)
    }

    /**
     * Add a new node to the cluster. Only leaders can perform this operation.
     */
    fun addNode(input: AddNode.Input): AddNode.Output

    object RemoveNode {
        data class Input(val requestId: String, val address: NodeAddress)
        data class Output(val leaderId: String?)
    }

    /**
     * Remove a node to the cluster. Only leaders can perform this operation.
     */
    fun removeNode(input: RemoveNode.Input): RemoveNode.Output
}
