package com.ackbox.raft.store

import com.ackbox.raft.support.SERIALIZER
import com.fasterxml.jackson.module.kotlin.readValue
import java.nio.ByteBuffer

/**
 * Kay-value representation of log data.
 */
data class KV(val key: String, val value: ByteBuffer) {

    fun toByteArray(): ByteArray = SERIALIZER.writeValueAsBytes(this)

    companion object {

        fun fromByteArray(data: ByteArray): KV = SERIALIZER.readValue(data)
    }
}
