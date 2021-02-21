package com.ackbox.raft.core

import com.ackbox.raft.types.Index
import com.ackbox.raft.types.UNDEFINED_ID

data class RemoteNodeState(
    /**
     * For each server, index of the next log entry to send to that server (initialized to leader last log index + 1).
     */
    val nextLogIndex: Index = Index(UNDEFINED_ID + 1),

    /**
     * For each server, index of highest log entry known to be replicated on server (initialized to 0, increases
     * monotonically).
     */
    val matchLogIndex: Index = Index.UNDEFINED
)
