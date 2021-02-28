package com.ackbox.raft.store

import com.ackbox.raft.support.SERIALIZER
import com.fasterxml.jackson.module.kotlin.readValue

/**
 * Kay-value representation of log data.
 */
data class KV(val key: String, val value: ByteArray) {

    fun toByteArray(): ByteArray = SERIALIZER.writeValueAsBytes(this)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        other as KV
        if (key != other.key) return false
        if (!value.contentEquals(other.value)) return false
        return true
    }

    override fun hashCode(): Int {
        var result = key.hashCode()
        result = 31 * result + value.contentHashCode()
        return result
    }

    companion object {

        fun fromByteArray(data: ByteArray): KV = SERIALIZER.readValue(data)
    }
}
