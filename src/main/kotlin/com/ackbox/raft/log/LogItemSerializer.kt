package com.ackbox.raft.log

import com.ackbox.raft.state.Index
import com.ackbox.raft.state.Term
import java.nio.ByteBuffer

object LogItemSerializer {

    const val HEADER_ITEM_SIZE_BYTES = Int.SIZE_BYTES
    const val HEADER_CRC_SIZE_BYTES = Long.SIZE_BYTES
    const val HEADER_SIZE_BYTES = HEADER_ITEM_SIZE_BYTES + HEADER_CRC_SIZE_BYTES

    fun toByteBuffer(item: LogItem): ByteBuffer {
        val buffer = ByteBuffer.allocate(item.getSizeInBytes())
        buffer.putLong(item.index.value)
        buffer.putLong(item.term.value)
        buffer.put(item.value)
        buffer.flip()
        return buffer
    }

    fun fromByteBuffer(buffer: ByteBuffer): LogItem {
        val index = buffer.long
        val term = buffer.long
        val value = ByteArray(buffer.remaining())
        buffer.get(value)
        return LogItem(Index(index), Term(term), value)
    }
}
