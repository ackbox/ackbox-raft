package com.ackbox.raft.log

import org.slf4j.LoggerFactory

/**
 * Interface for Raft's replicated log.
 */
interface ReplicatedLog {

    /**
     * Restore log from persistent storage.
     */
    fun open()

    /**
     * Safely close any used resources.
     */
    fun close()

    /**
     * Describe state of log for troubleshooting purposes.
     */
    fun describe()

    /**
     * Index of lowest log entry added to the log (initialized to 0, increases monotonically).
     */
    fun getFirstItemIndex(): Long

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
    fun appendItems(items: List<LogItem>)

    /**
     * Reset log to it's initial state by removing all in-memory and persisted entries.
     */
    fun clear()

    /**
     * Truncate the log at the [index]. This means that all entries before [index] (not including) will be
     * remove from the log.
     */
    fun truncateBefore(index: Long)

    /**
     * Truncate the log at the [index]. This means that all entries after [index] (not including) will be
     * remove from the log.
     */
    fun truncateAfter(index: Long)

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
