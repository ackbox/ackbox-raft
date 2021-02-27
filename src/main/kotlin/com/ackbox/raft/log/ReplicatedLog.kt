package com.ackbox.raft.log

import com.ackbox.raft.types.Index
import com.ackbox.raft.types.LogItem
import com.ackbox.raft.types.Term
import org.slf4j.Logger

/**
 * Interface for Raft's replicated log.
 */
interface ReplicatedLog {

    val logger: Logger

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
    fun getFirstItemIndex(): Index

    /**
     * Index of highest log entry added to the log (initialized to 0, increases monotonically).
     */
    fun getLastItemIndex(): Index

    /**
     * Get log item stored with [index].
     */
    fun getItem(index: Index): LogItem?

    /**
     * Append log entries received by leader (first index is 1).
     */
    fun appendItems(items: List<LogItem>)

    /**
     * Reset log to it's initial state by removing all in-memory and persisted entries.
     */
    fun clear()

    /**
     * Truncate the log at the [index]. This means that all entries before [index] (non-inclusive) will be
     * remove from the log.
     */
    fun truncateBeforeNonInclusive(index: Index)

    /**
     * Truncate the log at the [index]. This means that all entries after [index] (inclusive) will be
     * remove from the log.
     */
    fun truncateAfterInclusive(index: Index)

    /**
     * Check whether the log contains an entry at [externalIndex] matching [externalTerm].
     */
    fun containsItem(externalIndex: Index, externalTerm: Term): Boolean {
        if (externalIndex.isUndefined()) {
            val internalTerm = getItem(getLastItemIndex())?.term ?: Term.UNDEFINED
            logger.info("Marker log entry check: externalTerm=[{}] and internalTerm=[{}]", externalTerm, internalTerm)
            return true
        }
        val lastItemIndex = getLastItemIndex()
        if (externalIndex > lastItemIndex) {
            logger.info("Log is not caught up: externalIndex=[{}] and internalIndex=[{}]", externalIndex, lastItemIndex)
            return false
        }
        val entry = getItem(externalIndex)
        if (entry == null || entry.term != externalTerm) {
            val term = entry?.term ?: Term.UNDEFINED
            logger.info("Log is not caught up: externalTerm=[{}] and internalTerm=[{}]", externalTerm, term)
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
    fun isAheadOf(externalIndex: Index, externalTerm: Term): Boolean {
        val item = getItem(getLastItemIndex())
        if (item != null) {
            if (item.term != externalTerm) {
                val isAhead = item.term > externalTerm
                logger.info("Checking log entry term: logAhead=[{}]", isAhead)
                return isAhead
            }
            val isAhead = item.index > externalIndex
            logger.info("Checking log entry index: logAhead=[{}]", isAhead)
            return isAhead
        }
        logger.info("Log entry at externalTerm=[{}] is null", externalTerm)
        return false
    }
}
