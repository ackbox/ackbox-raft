package com.ackbox.raft.types

/**
 * Data class representing a log item.
 */
data class LogItem(val type: Type, val index: Index, val term: Term, val value: ByteArray) {

    enum class Type { STORE_CHANGE, NETWORKING_CHANGE }

    fun getSizeInBytes(): Int = Int.SIZE_BYTES + 2 * Long.SIZE_BYTES + value.size

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        other as LogItem
        if (index != other.index) return false
        if (term != other.term) return false
        if (!value.contentEquals(other.value)) return false
        return true
    }

    override fun hashCode(): Int {
        var result = index.hashCode()
        result = 31 * result + term.hashCode()
        result = 31 * result + value.contentHashCode()
        return result
    }

    override fun toString(): String {
        return "${LogItem::class.simpleName}(type=$type, index=${index.value}, term=${term.value}, size=${value.size})"
    }
}
