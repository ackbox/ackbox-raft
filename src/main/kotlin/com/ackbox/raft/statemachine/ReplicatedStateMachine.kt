package com.ackbox.raft.statemachine

import com.ackbox.raft.log.ReplicatedLog.LogItem

/**
 * Interface for Raft's replicated state machine.
 */
interface ReplicatedStateMachine {

    /**
     * Apply a log entry to the state machine. Each entry contains command for state machine, and term when
     * entry was received by leader (first index is 1).
     */
    fun setItem(item: LogItem)

    /**
     * Get an applied entry from the state machine.
     */
    fun getItem(index: Long): LogItem?
}
