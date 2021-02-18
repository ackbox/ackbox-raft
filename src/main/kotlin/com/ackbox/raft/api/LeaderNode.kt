package com.ackbox.raft.api

import java.nio.ByteBuffer

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
        data class Output(val leaderId: String?)
    }

    /**
     * Set an item to the replicated log. Only leaders can perform this operation.
     */
    fun setItem(input: Set.Input): Set.Output

    object Get {
        data class Input(val key: String)
        data class Output(val leaderId: String?, val data: ByteBuffer?)
    }

    /**
     * Get an item to the replicated log. Only leaders can perform this operation.
     */
    fun getItem(input: Get.Input): Get.Output
}
