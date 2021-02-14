package com.ackbox.raft.log

import java.nio.ByteBuffer

object LogItemSerializer {

    const val HEADER_ITEM_SIZE_BYTES = Int.SIZE_BYTES
    const val HEADER_CRC_SIZE_BYTES = Long.SIZE_BYTES
    const val HEADER_SIZE_BYTES = HEADER_ITEM_SIZE_BYTES + HEADER_CRC_SIZE_BYTES

    fun toByteBuffer(item: ReplicatedLog.LogItem): ByteBuffer {
        val buffer = ByteBuffer.allocate(item.getSizeInBytes())
        buffer.putLong(item.index)
        buffer.putLong(item.term)
        buffer.put(item.value)
        buffer.flip()
        return buffer
    }

    fun fromByteBuffer(buffer: ByteBuffer): ReplicatedLog.LogItem {
        val index = buffer.long
        val term = buffer.long
        val value = ByteArray(buffer.remaining())
        buffer.get(value)
        return ReplicatedLog.LogItem(index, term, value)
    }
}
