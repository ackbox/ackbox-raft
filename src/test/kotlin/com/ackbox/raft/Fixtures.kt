package com.ackbox.raft

import com.ackbox.raft.log.LogItem
import com.ackbox.raft.state.Index
import com.ackbox.raft.state.Term
import com.ackbox.random.krandom
import com.google.common.primitives.Longs

object Fixtures {

    private val DATA_8_BYTES: ByteArray = Longs.toByteArray(krandom())

    fun createLogItem(
        index: Index = Index.UNDEFINED,
        term: Term = Term.UNDEFINED,
        data: ByteArray = DATA_8_BYTES
    ): LogItem {
        return LogItem(index, term, data)
    }
}
