package com.ackbox.raft.log

import com.ackbox.raft.config.NodeConfig
import com.ackbox.raft.core.UNDEFINED_ID
import com.ackbox.raft.support.NodeLogger
import com.google.common.annotations.VisibleForTesting
import java.nio.file.Files
import java.nio.file.Path
import java.util.TreeMap
import javax.annotation.concurrent.NotThreadSafe
import kotlin.streams.asSequence

/**
 * Implementation of [ReplicatedLog] where node's log is represented by contiguous segments.
 * The current implementation ensures that log items are stored in persisted storage (file system).
 */
@NotThreadSafe
class SegmentedLog(private val config: NodeConfig) : ReplicatedLog {

    private val logger: NodeLogger = NodeLogger.from(config.nodeId, SegmentedLog::class)
    private val segments: TreeMap<Long, Segment> = TreeMap<Long, Segment>()

    override fun open() {
        logger.info("Opening segments from [{}]", config.logPath)
        Files.createDirectories(config.logPath)
        Files.walk(config.logPath).asSequence()
            .filter<Path>(Files::isRegularFile)
            .map { path -> Segment.getFirstIndexFromFilename(path.fileName.toString()) }
            .map { index -> Segment(index, config.logPath, config.maxLogSegmentSizeInBytes) }
            .onEach { segment -> segment.load() }
            .forEach { segment -> segments[segment.firstItemIndex] = segment }
        segments.lastEntry()?.value?.open()
    }

    override fun close() {
        logger.info("Closing segments from [{}]", config.logPath)
        segments.values.forEach { segment -> segment.safelyClose() }
    }

    override fun describe() {
        open()
        segments.entries.forEach { (_, segment) ->
            logger.debug("Describing segment [{}]", segment)
            logger.debug(segment.describe())
        }
        close()
    }

    override fun getFirstItemIndex(): Long {
        val firstSegment = segments.firstEntry()?.value
        return firstSegment?.firstItemIndex ?: UNDEFINED_ID
    }

    override fun getLastItemIndex(): Long {
        val lastSegment = segments.lastEntry()?.value
        return lastSegment?.lastItemIndex ?: UNDEFINED_ID
    }

    override fun getItem(index: Long): LogItem? {
        logger.debug("Getting item at [{}]", index)
        return getSegmentFor(index)?.get(index)
    }

    override fun appendItems(items: List<LogItem>) {
        logger.debug("First segment [{}]", segments.firstEntry()?.value)
        logger.debug("Last segment [{}]", segments.lastEntry()?.value)
        items.forEach { item ->
            val currentItem = getItem(item.index)
            if (currentItem == null) {
                // Standard case where items are appended to the log sequentially.
                // Internal checks in the segment will ensure this invariant.
                logger.debug("Appending new item [{}]", item)
                ensureSegmentFor(item).append(item)
            } else if (currentItem.term != item.term) {
                // Failure case where a node was ahead of the leader due to some failure
                // condition. Once the new leader is confirmed, it will force all followers
                // to replicate its log. Here we ensure that the follower is able to correctly
                // truncate the log at the divergence point before appending the entry.
                logger.warn("Truncating log at index [{}] due to term mismatch", item.index)
                truncateAfterInclusive(item.index)
                ensureSegmentFor(item).append(item)
            } else {
                logger.info("Ignoring item since it already exists in log: item=[{}]", item)
            }
        }
    }

    override fun clear() {
        logger.warn("Deleting all log entries from memory and file system")
        segments.values.forEach { segment -> segment.safelyDelete() }
        segments.clear()
    }

    override fun truncateBeforeNonInclusive(index: Long) {
        logger.info("Truncating logs before index [{}]", index)
        val toRemove = segments.navigableKeySet().headSet(index).toSet()
        toRemove.forEach { segmentIndex ->
            val segmentToRemove = segments.remove(segmentIndex)
            segmentToRemove?.safelyClose()
            segmentToRemove?.safelyDelete()
        }
    }

    override fun truncateAfterInclusive(index: Long) {
        logger.info("Truncating logs after index [{}]", index)
        val toRemove = segments.navigableKeySet().tailSet(index).toSet()
        toRemove.forEach { segmentIndex ->
            val segmentToRemove = segments.remove(segmentIndex)
            segmentToRemove?.safelyClose()
            segmentToRemove?.safelyDelete()
        }
        getSegmentFor(index )?.open()?.truncateAt(index)
    }

    @VisibleForTesting
    fun getSegmentSize(): Int = segments.size

    private fun ensureSegmentFor(item: LogItem): Segment {
        val lastSegment = segments.lastEntry()?.value
        if (lastSegment?.canFit(item) != true) {
            // Last segment is either null or cannot fit the item. Whenever the segment
            // cannot store the new item or the segment is null (potentially on startup)
            // the current segment is closed and a new one is created.
            logger.info("Creating a new segment for index [{}]", item.index)
            lastSegment?.safelyClose()
            val logPath = config.logPath
            val maxSizeInBytes = config.maxLogSegmentSizeInBytes
            segments[item.index] = Segment(item.index, logPath, maxSizeInBytes)
        }
        return segments.lastEntry()!!.value.open()
    }

    private fun getSegmentFor(index: Long): Segment? {
        val firstLogIndex = getFirstItemIndex()
        val lastLogIndex = getLastItemIndex()
        if (index < firstLogIndex || index > lastLogIndex) {
            // In this case, an entry was requested but its index is not present
            // in any of the segments in this log.
            logger.info("Item index=[{}] outside log boundaries: [{}]::[{}]", index, firstLogIndex, lastLogIndex)
            return null
        }
        val segment = segments.floorEntry(index)
        logger.debug("Found item at segment=[{}] and index=[{}]", segment?.value, index)
        return segment?.value
    }

    private fun Segment.safelyClose() {
        try {
            logger.info("Closing segment [{}]", this)
            close()
        } catch (e: Exception) {
            logger.error("Error while closing segment [{}]", this)
        }
    }

    private fun Segment.safelyDelete() {
        try {
            logger.info("Deleting segment for [{}]", this)
            delete()
        } catch (e: Exception) {
            logger.error("Error while deleting segment [{}]", this)
        }
    }
}
