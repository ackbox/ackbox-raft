package com.ackbox.raft

import com.ackbox.raft.log.LogItem
import com.ackbox.random.krandom
import com.google.common.primitives.Longs

object Fixtures {

    private const val TERM: Long = 0
    private val DATA_8_BYTES: ByteArray = Longs.toByteArray(krandom())

    fun createLogItem(index: Long, term: Long = TERM, data: ByteArray = DATA_8_BYTES): LogItem {
        return LogItem(index, term, data)
    }
}
