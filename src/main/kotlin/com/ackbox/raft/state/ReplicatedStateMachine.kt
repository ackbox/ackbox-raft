package com.ackbox.raft.state

import com.ackbox.raft.state.ReplicatedLog.LogItem
import com.ackbox.raft.support.NodeLogger

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

class InMemoryReplicatedStateMachine(nodeId: String) : ReplicatedStateMachine {

    private val logger = NodeLogger.from(nodeId, InMemoryReplicatedLog::class)
    private val commands: MutableMap<Long, LogItem> = mutableMapOf()

    override fun setItem(item: LogItem) {
        logger.info("Setting item with index=[{}]", item.index)
        commands[item.index] = item
    }

    override fun getItem(index: Long): LogItem? {
        logger.info("Getting item with index=[{}]", index)
        return commands[index]
    }
}
