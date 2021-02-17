package com.ackbox.raft.state

import com.ackbox.raft.core.UNDEFINED_ID

data class CommitMetadata(
    /**
     * Index of highest log entry applied to state machine (initialized to 0, increases monotonically).
     */
    val lastAppliedLogIndex: Long = UNDEFINED_ID,

    /**
     * Term of highest log entry applied to state machine (initialized to 0).
     */
    val lastAppliedLogTerm: Long = UNDEFINED_ID,

    /**
     * Index of highest log entry known to be committed (initialized to 0, increases monotonically).
     */
    val commitIndex: Long = UNDEFINED_ID
)
