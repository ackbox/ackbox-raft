package com.ackbox.raft.statemachine

import com.ackbox.raft.log.LogItem

/**
 * Interface for Raft's replicated state machine.
 */
interface ReplicatedStateMachine {

    /**
     * Index of highest log entry applied to state machine (initialized to 0, increases monotonically).
     */
    fun getLastAppliedLogIndex(): Long

    /**
     * Term of highest log entry applied to state machine.
     */
    fun getLastAppliedLogTerm(): Long

    /**
     * Apply a log entry to the state machine. Each entry contains command for state machine, and term when
     * entry was received by leader (first index is 1).
     */
    fun setItem(item: LogItem)

    /**
     * Get an applied entry from the state machine.
     */
    fun getItem(index: Long): LogItem?

    /**
     * Take a snapshot of the state machine's state. The resulting snapshot should be consistent. The
     * algorithm implementation will ensure that any mutation operation is blocked until this method
     * returns. This means that the implementation of this method does not need to be concerned about
     * concurrency issues.
     */
    fun takeSnapshot(): Snapshot

    /**
     * Load a snapshot of the state machine's state. The snapshot loading is atomic, meaning that the
     * entire state machine's state will have been replaced by the snapshot provided once the method
     * returns. The algorithm implementation will ensure that any mutation operation is blocked until
     * this method returns. This means that the implementation of this method does not need to be
     * concerned about concurrency issues.
     */
    fun restoreSnapshot(snapshot: Snapshot)
}
