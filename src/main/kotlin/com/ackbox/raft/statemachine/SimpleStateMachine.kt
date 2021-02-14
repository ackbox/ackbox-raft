package com.ackbox.raft.statemachine

import com.ackbox.raft.log.ReplicatedLog
import com.ackbox.raft.support.NodeLogger

class SimpleStateMachine(nodeId: String) : ReplicatedStateMachine {

    private val logger: NodeLogger = NodeLogger.from(nodeId, SimpleStateMachine::class)
    private val commands: MutableMap<Long, ReplicatedLog.LogItem> = mutableMapOf()

    override fun setItem(item: ReplicatedLog.LogItem) {
        logger.info("Setting item with index=[{}]", item.index)
        commands[item.index] = item
    }

    override fun getItem(index: Long): ReplicatedLog.LogItem? {
        logger.info("Getting item with index=[{}]", index)
        return commands[index]
    }
}
