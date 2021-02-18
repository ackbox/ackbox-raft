package com.ackbox.raft.types

/**
 * Class defining the consensus metadata.
 */
data class ConsensusMetadata(
    /**
     * Latest term server has seen (initialized to 0 on first boot, increases monotonically).
     */
    val currentTerm: Term = Term.UNDEFINED,

    /**
     * NodeId that is said to be the leader in current term (or null if none).
     */
    val leaderId: String? = null,

    /**
     * CandidateId that received vote in current term (or null if none).
     */
    val votedFor: String? = null,

    /**
     * Current operation mode of the node in the term.
     */
    val mode: NodeMode = NodeMode.FOLLOWER
)
