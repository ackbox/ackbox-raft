package com.ackbox.raft.log

import com.ackbox.raft.core.Randoms
import com.ackbox.random.krandom
import com.google.common.primitives.Longs
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir
import java.io.File

internal class SegmentTest {

    @TempDir // Needs to be non-private.
    internal lateinit var baseFolder: File

    @Test
    fun `should reject append of segment is not open`() {
        val firstIndex = System.nanoTime()
        val firstItem = createLogItem(firstIndex)
        val secondItem = createLogItem(firstIndex + 1)
        val segment = Segment(firstIndex, baseFolder.toPath(), SIZE_1024).load()

        segment.open()
        assertDoesNotThrow { segment.append(firstItem) }
        segment.close()
        assertThrows<IllegalStateException> { segment.append(secondItem) }
    }

    @Test
    fun `should reject append of non-sequential log items`() {
        val firstIndex = System.nanoTime()
        val firstItem = createLogItem(firstIndex)
        val secondItem = createLogItem(firstIndex + 1)
        val outOfSequenceItem = createLogItem(firstIndex + INDEX_DIFF)
        val segment = Segment(firstIndex, baseFolder.toPath(), SIZE_1024).open()
        assertSegmentInitialState(firstIndex, segment)

        segment.append(firstItem)
        assertItemStorage(firstItem, segment)

        segment.append(secondItem)
        assertItemStorage(secondItem, segment)

        assertThrows<IllegalStateException> { segment.append(outOfSequenceItem) }
        segment.close()
    }

    @Test
    fun `should reject get of out-of-bounds log items`() {
        val firstIndex = System.nanoTime()
        val firstItem = createLogItem(firstIndex)
        val outOfLowerBoundItem = createLogItem(firstIndex - INDEX_DIFF)
        val outOfUpperBoundItem = createLogItem(firstIndex + INDEX_DIFF)
        val segment = Segment(firstIndex, baseFolder.toPath(), SIZE_1024).open()
        assertSegmentInitialState(firstIndex, segment)

        segment.append(firstItem)
        segment.close()

        assertEquals(firstItem.index, segment.firstItemIndex)
        assertItemStorage(firstItem, segment)
        assertThrows<IllegalStateException> { segment.get(outOfLowerBoundItem.index) }
        assertThrows<IllegalStateException> { segment.get(outOfUpperBoundItem.index) }
    }

    @Test
    fun `should create segment from non-existent file`() {
        val firstIndex = System.nanoTime()
        val item = createLogItem(firstIndex)
        val segment = Segment(firstIndex, baseFolder.toPath(), SIZE_1024).load().open()
        assertSegmentInitialState(firstIndex, segment)

        assertEquals(firstIndex, segment.firstItemIndex)
        assertEquals(firstIndex - 1, segment.lastItemIndex) // Since it's empty.
        segment.append(item)

        assertEquals(firstIndex, segment.firstItemIndex)
        assertItemStorage(item, segment)
        segment.close()
    }

    @Test
    fun `should load segment from file`() {
        val firstIndex = System.nanoTime()
        val items = (0 until Randoms.between(5, 50)).map { offset -> createLogItem(firstIndex + offset) }

        val segment1 = Segment(firstIndex, baseFolder.toPath(), SIZE_1024).open()
        items.forEach { item -> segment1.append(item) }
        segment1.close()

        val segment2 = Segment(firstIndex, baseFolder.toPath(), SIZE_1024).load()
        items.forEach { item -> assertEquals(item, segment2.get(item.index)) }
    }

    @Test
    fun `should append to segment after loading from file`() {
        val firstIndex = System.nanoTime()
        val firstOffset = Randoms.between(5, 10)
        val secondOffset = Randoms.between(10, 20)
        val items = (0 until firstOffset).map { offset -> createLogItem(firstIndex + offset) }
        val extraItems = (firstOffset until secondOffset).map { offset -> createLogItem(firstIndex + offset) }

        val segment1 = Segment(firstIndex, baseFolder.toPath(), SIZE_1024).open()
        items.forEach { item -> segment1.append(item) }
        segment1.close()

        val segment2 = Segment(firstIndex, baseFolder.toPath(), SIZE_1024).load().open()
        extraItems.forEach { item -> segment2.append(item) }
        items.forEach { item -> assertEquals(item, segment2.get(item.index)) }
        extraItems.forEach { item -> assertEquals(item, segment2.get(item.index)) }
        segment2.close()
    }

    @Test
    fun `should reject item if segment is full`() {
        val firstIndex = System.nanoTime()
        val item1 = createLogItem(firstIndex)
        val item2 = createLogItem(firstIndex + 1)
        val singleItemDataSize = LogItemSerializer.HEADER_SIZE_BYTES + item1.getSizeInBytes()

        val segment = Segment(firstIndex, baseFolder.toPath(), singleItemDataSize).open()
        assertTrue(segment.canFit(item1))
        segment.append(item1)
        assertFalse(segment.canFit(item2))
        segment.close()
    }

    @Test
    fun `should be able to truncate at a specific index`() {
        val firstIndex = System.nanoTime()
        val firstOffset = Randoms.between(5, 10)
        val secondOffset = Randoms.between(10, 20)
        val items = (0 until firstOffset).map { offset -> createLogItem(firstIndex + offset) }
        val tailItems1 = (firstOffset until secondOffset).map { offset -> createLogItem(firstIndex + offset) }
        val tailItems2 = (firstOffset until secondOffset).map { offset -> createLogItem(firstIndex + offset) }

        val segment = Segment(firstIndex, baseFolder.toPath(), SIZE_1024).open()
        items.forEach { item -> segment.append(item) }
        tailItems1.forEach { item -> segment.append(item) }

        val midIndex = tailItems1.first().index
        segment.truncateAt(midIndex)
        tailItems2.forEach { item -> segment.append(item) }

        (items + tailItems2).forEach { item -> assertEquals(item, segment.get(item.index)) }
        segment.close()
    }

    private fun assertSegmentInitialState(firstIndex: Long, segment: Segment) {
        assertEquals(firstIndex, segment.firstItemIndex)
        assertEquals(firstIndex, segment.lastItemIndex + 1)
    }

    private fun assertItemStorage(item: LogItem, segment: Segment) {
        assertEquals(item.index, segment.lastItemIndex)
        assertEquals(item, segment.get(item.index))
    }

    private fun createLogItem(index: Long): LogItem {
        return LogItem(index, TERM, DATA_8_BYTES)
    }

    companion object {

        private const val INDEX_DIFF: Long = 5
        private const val TERM: Long = 0
        private const val SIZE_1024: Int = 1024
        private val DATA_8_BYTES = Longs.toByteArray(krandom())
    }
}
