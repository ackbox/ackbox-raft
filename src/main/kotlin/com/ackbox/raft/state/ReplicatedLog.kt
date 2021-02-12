package com.ackbox.raft.state

import com.ackbox.raft.core.UNDEFINED_ID
import com.ackbox.raft.support.NodeLogger
import org.slf4j.LoggerFactory
import java.nio.ByteBuffer

/**
 * Interface for Raft's replicated log.
 */
interface ReplicatedLog {

    data class LogItem(val index: Long, val term: Long, val value: ByteBuffer)

    /**
     * Index of highest log entry added to the log (initialized to 0, increases monotonically).
     */
    fun getLastItemIndex(): Long

    /**
     * Get log item stored with [index].
     */
    fun getItem(index: Long): LogItem?

    /**
     * Append log entries received by leader (first index is 1).
     */
    fun appendItems(toAppend: List<LogItem>)

    /**
     * Check whether the log contains an entry at [externalIndex] matching [externalTerm].
     */
    fun containsItem(externalIndex: Long, externalTerm: Long): Boolean {
        val lastItemIndex = getLastItemIndex()
        if (externalIndex > lastItemIndex) {
            LOG.info("Log is not caught up: externalIndex=[{}], internalIndex=[{}]", externalIndex, lastItemIndex)
            return false
        }
        val entry = getItem(externalIndex)
        if (entry == null || entry.term != externalTerm) {
            LOG.info("Log is not caught up: externalTerm=[{}], internalTerm=[{}]", externalTerm, entry?.term)
            return false
        }
        return true
    }

    /**
     * Check whether the log is ahead of an external log represented by [externalIndex] and [externalTerm].
     *
     * Raft determines which of two logs is more up-to-date by comparing the index and term of the last
     * entries in the logs. If the logs have last entries with different terms, then the log with the
     * later term is more up-to-date. If the logs end with the same term, then whichever log is longer is
     * more up-to-date.
     */
    fun isAheadOf(externalIndex: Long, externalTerm: Long): Boolean {
        val item = getItem(getLastItemIndex())
        if (item != null) {
            if (item.term != externalTerm) {
                val isAhead = item.term > externalTerm
                LOG.info("Checking log entry term: logAhead=[{}]", isAhead)
                return isAhead
            }
            val isAhead = item.index > externalIndex
            LOG.info("Checking log entry index: logAhead=[{}]", isAhead)
            return isAhead
        }
        LOG.info("Log entry at externalTerm=[{}] is null", externalTerm)
        return false
    }

    companion object {

        private val LOG = LoggerFactory.getLogger(ReplicatedLog::class.java)
    }
}

class InMemoryReplicatedLog(nodeId: String) : ReplicatedLog {

    private val logger = NodeLogger.from(nodeId, InMemoryReplicatedLog::class)
    private var lastLogIndex: Long = UNDEFINED_ID
    private val items: MutableMap<Long, ReplicatedLog.LogItem> = mutableMapOf()

    init {
        // Ensure log has marker item to avoid testing for indexes that do not exist.
        items[UNDEFINED_ID] = ReplicatedLog.LogItem(UNDEFINED_ID, UNDEFINED_ID, ByteBuffer.allocate(0))
    }

    override fun getLastItemIndex(): Long = lastLogIndex

    override fun getItem(index: Long): ReplicatedLog.LogItem? = items[index]

    override fun appendItems(toAppend: List<ReplicatedLog.LogItem>) {
        logger.info("Appending [{}] entries to the log", toAppend.size)
        toAppend.forEach { item ->
            if (getItem(item.index)?.term != item.term) {
                // TODO: truncate the log items at [index - 1].
            }
            items[item.index] = item
            lastLogIndex = item.index
        }
    }
}
