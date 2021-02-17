package com.ackbox.raft.state

data class CommitMetadata(
    /**
     * Index of highest log entry applied to state machine (initialized to 0, increases monotonically).
     */
    val lastAppliedLogIndex: Index = Index.UNDEFINED,

    /**
     * Term of highest log entry applied to state machine (initialized to 0).
     */
    val lastAppliedLogTerm: Term = Term.UNDEFINED,

    /**
     * Index of highest log entry known to be committed (initialized to 0, increases monotonically).
     */
    val commitIndex: Index = Index.UNDEFINED
)
