package com.ackbox.raft.statemachine

import com.ackbox.raft.config.NodeConfig
import com.ackbox.raft.core.UNDEFINED_ID
import com.ackbox.raft.log.LogItem
import com.ackbox.raft.support.NodeLogger
import java.io.File
import javax.annotation.concurrent.NotThreadSafe

@NotThreadSafe
class SimpleStateMachine(config: NodeConfig) : ReplicatedStateMachine {

    private val logger: NodeLogger = NodeLogger.from(config.nodeId, SimpleStateMachine::class)
    private val commands: MutableMap<Long, LogItem> = mutableMapOf()

    private var lastAppliedLogIndex: Long = UNDEFINED_ID
    private var lastAppliedLogTerm: Long = UNDEFINED_ID

    override fun getLastAppliedLogIndex(): Long = lastAppliedLogIndex

    override fun getLastAppliedLogTerm(): Long = lastAppliedLogTerm

    override fun setItem(item: LogItem) {
        logger.info("Setting item with index=[{}]", item.index)
        commands[item.index] = item
        lastAppliedLogIndex = item.index
    }

    override fun getItem(index: Long): LogItem? {
        logger.info("Getting item with index=[{}]", index)
        return commands[index]
    }

    override fun takeSnapshot(): Snapshot {
        // TODO: Save snapshot somewhere.
        logger.info("Taking a snapshot at index=[{}] and term=[{}]", lastAppliedLogIndex, lastAppliedLogTerm)
        val prefix = "snapshot-${System.currentTimeMillis()}"
        val dataPath = File.createTempFile(prefix, "snapshot.temp").toPath()
        return Snapshot(lastAppliedLogIndex, lastAppliedLogTerm, dataPath)
    }

    override fun restoreSnapshot(snapshot: Snapshot) {
        lastAppliedLogIndex = snapshot.lastIncludedLogIndex
        lastAppliedLogTerm = snapshot.lastIncludedLogTerm
        // TODO: Do something with `snapshot.dataPath`.
        // snapshot.dataPath
    }
}
