package com.ackbox.raft

import com.ackbox.raft.types.LogItem
import com.ackbox.raft.types.Index
import com.ackbox.raft.types.Term
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
