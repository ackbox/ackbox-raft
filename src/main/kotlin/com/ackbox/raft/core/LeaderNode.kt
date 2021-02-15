package com.ackbox.raft.core

import com.ackbox.raft.log.LogItem

/**
 * Interface for nodes acting as leader in the cluster.
 */
interface LeaderNode {

    /**
     * Unique identifier of this node.
     */
    val nodeId: String

    object Set {
        data class Input(val data: List<ByteArray>)
        data class Output(val leaderId: String?, val itemSqn: Long)
    }

    /**
     * Set an item to the replicated log. Only leaders can perform this operation.
     */
    fun setItem(input: Set.Input): Set.Output

    object Get {
        data class Input(val itemSqn: Long)
        data class Output(val leaderId: String?, val item: LogItem?)
    }

    /**
     * Get an item to the replicated log. Only leaders can perform this operation.
     */
    fun getItem(input: Get.Input): Get.Output
}
