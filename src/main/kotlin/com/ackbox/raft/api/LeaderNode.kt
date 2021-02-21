package com.ackbox.raft.api

import com.ackbox.raft.networking.NodeAddress
import com.ackbox.raft.types.LogItem
import java.nio.ByteBuffer

/**
 * Interface for nodes acting as leader in the cluster.
 */
interface LeaderNode {

    /**
     * Unique identifier of this node.
     */
    val nodeId: String

    object SetItem {
        data class Input(val type: LogItem.Type, val data: List<ByteArray>)
        data class Output(val leaderId: String?)
    }

    /**
     * Set an item to the replicated log. Only leaders can perform this operation.
     */
    fun setItem(input: SetItem.Input): SetItem.Output

    object GetItem {
        data class Input(val key: String)
        data class Output(val leaderId: String?, val data: ByteBuffer?)
    }

    /**
     * Get an item to the replicated log. Only leaders can perform this operation.
     */
    fun getItem(input: GetItem.Input): GetItem.Output

    object AddNode {
        data class Input(val address: NodeAddress)
        data class Output(val leaderId: String?)
    }

    /**
     * Add a new node to the cluster. Only leaders can perform this operation.
     */
    fun addNode(input: AddNode.Input): AddNode.Output

    object RemoveNode {
        data class Input(val address: NodeAddress)
        data class Output(val leaderId: String?)
    }

    /**
     * Remove a node to the cluster. Only leaders can perform this operation.
     */
    fun removeNode(input: RemoveNode.Input): RemoveNode.Output
}
