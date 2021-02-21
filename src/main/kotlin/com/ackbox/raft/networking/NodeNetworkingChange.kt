package com.ackbox.raft.networking

import com.ackbox.raft.support.SERIALIZER
import com.fasterxml.jackson.module.kotlin.readValue

data class NodeNetworkingChange(val type: Type, val address: NodeAddress) {

    enum class Type { ADDED, REMOVED }

    fun toByteArray(): ByteArray = SERIALIZER.writeValueAsBytes(this)

    companion object {

        fun fromByteArray(data: ByteArray): NodeNetworkingChange = SERIALIZER.readValue(data)
    }
}
