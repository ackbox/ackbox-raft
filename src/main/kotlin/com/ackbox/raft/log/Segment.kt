package com.ackbox.raft.log

import com.ackbox.raft.log.LogItemSerializer.HEADER_CRC_SIZE_BYTES
import com.ackbox.raft.log.LogItemSerializer.HEADER_ITEM_SIZE_BYTES
import com.ackbox.raft.log.LogItemSerializer.HEADER_SIZE_BYTES
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import java.util.zip.CRC32

data class Segment(val firstItemIndex: Long, private val path: Path, private val maxSizeInBytes: Int) {

    private var channel: FileChannel? = null
    private var offsetInBytes: Long = 0
    private val items: MutableList<SegmentEntry> = mutableListOf()

    /**
     * Index of last item stored in the segment. Segment is created without any entries.
     * At first last item index is set to [firstItemIndex - 1]
     */
    var lastItemIndex: Long = firstItemIndex - 1
        private set

    fun canFit(item: LogItem): Boolean = isOpen() && offsetInBytes + item.getSizeInBytes() <= maxSizeInBytes

    fun isEmpty(): Boolean = lastItemIndex - firstItemIndex < 0

    fun append(item: LogItem) {
        ensureSegmentOpen()
        ensureSequentialInvariant(item)
        channel?.writeItem(item)
        items.add(SegmentEntry(item, offsetInBytes))
        lastItemIndex = item.index
        offsetInBytes = channel?.position() ?: 0
    }

    fun get(index: Long): LogItem? {
        return getEntry(index)?.item
    }

    fun truncateAt(index: Long) {
        ensureSegmentOpen()
        val entry = getEntry(index)
        entry?.let {
            // NOTE: For optimization reasons, we do not remove items from [items]
            // list as they will be automatically overwritten in subsequent operations.
            channel?.truncate(it.byteOffset)
            offsetInBytes = it.byteOffset
            lastItemIndex = index - 1
        }
    }

    fun open(): Segment {
        if (channel == null) {
            channel = createChannel()
            channel?.position(offsetInBytes)
        }
        return this
    }

    fun load(): Segment {
        createChannel().use { channel ->
            while (channel.position() < channel.size()) {
                // NOTE: byteOffset should be retrieved before the channel read.
                val byteOffset = channel.position()
                val item = channel.readItem()
                items.add(SegmentEntry(item, byteOffset))
                lastItemIndex = item.index
            }
            offsetInBytes = channel.position()
        }
        return this
    }

    fun close(): Segment {
        if (isOpen()) {
            channel?.close()
            channel = null
        }
        return this
    }

    fun isOpen(): Boolean = channel != null

    fun describe(): String {
        return items.joinToString("") { item -> "\nSegment item [${item}]" }
    }

    fun delete() {
        createPath().toFile().delete()
    }

    override fun toString(): String {
        val status = if (isOpen()) "open" else "closed"
        return "Segment range=[$firstItemIndex]::[$lastItemIndex] size=[${items.size}] status=[$status]"
    }

    private fun getEntry(index: Long): SegmentEntry? {
        check(index >= firstItemIndex) { "Index [$index] must be greater or equal to firstIndex=[$firstItemIndex]" }
        val adjustedIndex = index - firstItemIndex
        return items.getOrNull(adjustedIndex.toInt())
    }

    private fun createChannel(): FileChannel {
        return FileChannel.open(
            createPath(),
            StandardOpenOption.CREATE,
            StandardOpenOption.READ,
            StandardOpenOption.WRITE
        )
    }

    private fun createPath(): Path = Paths.get(path.toString(), getFilename())

    private fun ensureSegmentOpen() {
        check(isOpen()) { "Segment [${getFilename()}] is not open" }
    }

    private fun ensureSequentialInvariant(item: LogItem) {
        check(item.index == lastItemIndex + 1) { "Expected index=[${lastItemIndex + 1}] but got index=[${item.index}]" }
    }

    private fun computeCrc(data: ByteBuffer): Long {
        return CRC32().apply { update(data.array()) }.value
    }

    private fun FileChannel.writeItem(item: LogItem) {
        // NOTE: Order in which methods are called is important.
        val itemSizeInBytes = item.getSizeInBytes()
        val itemData = LogItemSerializer.toByteBuffer(item)
        val crc = computeCrc(itemData)
        val buffer = ByteBuffer.allocate(HEADER_SIZE_BYTES + itemSizeInBytes)
        // Write header data.
        buffer.putInt(itemSizeInBytes)
        buffer.putLong(crc)
        // Write payload data.
        buffer.put(itemData)
        buffer.flip()
        write(buffer)
    }

    private fun FileChannel.readItem(): LogItem {
        // NOTE: Order in which methods are called is important.
        // Read header data.
        val itemSizeBuffer = ByteBuffer.allocate(HEADER_ITEM_SIZE_BYTES)
        val crcBuffer = ByteBuffer.allocate(HEADER_CRC_SIZE_BYTES)
        read(itemSizeBuffer)
        val itemDataBuffer = ByteBuffer.allocate(itemSizeBuffer.apply { flip() }.int)
        read(crcBuffer)
        // Read payload data.
        read(itemDataBuffer)
        // Consistency check.
        val computedCrc = computeCrc(itemDataBuffer.apply { flip() })
        if (computedCrc != crcBuffer.apply { flip() }.long) {
            throw IllegalStateException("Corrupted segment at [${getFilename()}]")
        }
        return LogItemSerializer.fromByteBuffer(itemDataBuffer)
    }

    private fun getFilename(): String = "segment$SEPARATOR$firstItemIndex"

    private data class SegmentEntry(
        /**
         * Log item that is stored in the segment position.
         */
        val item: LogItem,
        /**
         * Since segments are contiguous, [byteOffset] represents the position (byte) in the persistent
         * storage where the entry starts. This is handy when segments are truncated since we simply need
         * to truncate the file at [byteOffset].
         */
        val byteOffset: Long
    )

    companion object {

        private const val SEPARATOR = "-"

        fun getFirstIndexFromFilename(filename: String): Long = filename.split(SEPARATOR).last().toLong()
    }
}
