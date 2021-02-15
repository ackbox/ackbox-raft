package com.ackbox.raft.log

import com.ackbox.raft.config.NodeConfig
import com.ackbox.raft.core.UNDEFINED_ID
import com.ackbox.raft.support.NodeLogger
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
        segments.values.forEach { segment -> closeSegment(segment) }
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
                val segment = getSegmentFor(item.index)!!
                segment.truncateAt(item.index)
                segment.append(item)
            } else {
                logger.info("Ignoring item since it already exists in log: item=[{}]", item)
            }
        }
    }

    override fun clear() {
        logger.warn("Deleting all log entries from memory and file system")
        segments.values.forEach { segment ->
            try {
                segment.delete()
            } catch (e: Exception) {
                logger.warn("Error while deleting segment [{}]", segment, e)
            }
        }
        segments.clear()
    }

    override fun truncateBefore(index: Long) {
        logger.info("Truncating logs at index [{}]", index)
        val segment = getSegmentFor(index) ?: return
        val toRemove = segments.keys.filter { segmentIndex -> segmentIndex < segment.firstItemIndex }
        toRemove.forEach { segmentIndex ->
            val segmentToRemove = segments.remove(segmentIndex)
            closeSegment(segmentToRemove)
            segmentToRemove?.delete()
        }
    }

    override fun truncateAfter(index: Long) {
        val segment = getSegmentFor(index)
        segment?.truncateAt(index)
    }

    private fun ensureSegmentFor(item: LogItem): Segment {
        val lastSegment = segments.lastEntry()?.value
        if (lastSegment?.canFit(item) != true) {
            // Last segment is either null or cannot fit the item. Whenever the segment
            // cannot store the new item or the segment is null (potentially on startup)
            // the current segment is closed and a new one is created.
            logger.info("Creating a new segment for index [{}]", item.index)
            closeSegment(lastSegment)
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

    private fun closeSegment(segment: Segment?) {
        try {
            logger.info("Closed segment for at [{}::{}]", segment?.firstItemIndex, segment?.lastItemIndex)
            segment?.close()
        } catch (e: Exception) {
            logger.error("Error while closing segment [{}]", segment?.getFilename())
        }
    }
}
