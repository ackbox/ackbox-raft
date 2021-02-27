package com.ackbox.raft.log

import com.ackbox.raft.Fixtures
import com.ackbox.raft.types.Index
import com.ackbox.raft.types.LogItem
import com.ackbox.raft.types.Randoms
import com.ackbox.raft.types.Term
import com.ackbox.raft.use
import com.ackbox.random.krandom
import com.google.common.primitives.Longs
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
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
        val firstIndex = Index(System.nanoTime())
        val firstItem = createLogItem(firstIndex)
        val secondItem = createLogItem(firstIndex.incremented())
        val segment = Segment(firstIndex, baseFolder.toPath(), SIZE_1024).load()

        segment.open()
        assertDoesNotThrow { segment.append(firstItem) }
        segment.close()
        assertThrows<IllegalStateException> { segment.append(secondItem) }
    }

    @Test
    fun `should reject append of non-sequential log items`() {
        val firstIndex = Index(System.nanoTime())
        val firstItem = createLogItem(firstIndex)
        val secondItem = createLogItem(firstIndex.incremented())
        val outOfSequenceItem = createLogItem(firstIndex.incrementedBy(INDEX_DIFF))
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
        val firstIndex = Index(System.nanoTime())
        val firstItem = createLogItem(firstIndex)
        val outOfBoundItem = createLogItem(firstIndex.decrementedBy(INDEX_DIFF))

        Segment(firstIndex, baseFolder.toPath(), SIZE_1024).use { segment ->
            assertSegmentInitialState(firstIndex, segment)
            segment.append(firstItem)

            assertEquals(firstItem.index, segment.firstItemIndex)
            assertItemStorage(firstItem, segment)
            assertThrows<IllegalStateException> { segment.get(outOfBoundItem.index) }
        }
    }

    @Test
    fun `should create segment from non-existent file`() {
        val firstIndex = Index(System.nanoTime())
        val item = createLogItem(firstIndex)
        val segment = Segment(firstIndex, baseFolder.toPath(), SIZE_1024).load().open()
        assertSegmentInitialState(firstIndex, segment)

        assertEquals(firstIndex, segment.firstItemIndex)
        assertEquals(firstIndex.decremented(), segment.lastItemIndex) // Since it's empty.
        segment.append(item)

        assertEquals(firstIndex, segment.firstItemIndex)
        assertItemStorage(item, segment)
        segment.close()
    }

    @Test
    fun `should load segment from file`() {
        val firstIndex = Index(System.nanoTime())
        val items = (0 until Randoms.between(5, 50)).map { offset ->
            createLogItem(firstIndex.incrementedBy(offset))
        }

        Segment(firstIndex, baseFolder.toPath(), SIZE_1024).use { segment ->
            items.forEach { item -> segment.append(item) }
        }

        val segment2 = Segment(firstIndex, baseFolder.toPath(), SIZE_1024).load()
        items.forEach { item -> assertEquals(item, segment2.get(item.index)) }
    }

    @Test
    fun `should append to segment after loading from file`() {
        val firstIndex = Index(System.nanoTime())
        val firstOffset = Randoms.between(5, 10)
        val secondOffset = Randoms.between(10, 20)
        val items = (0 until firstOffset).map { offset -> createLogItem(firstIndex.incrementedBy(offset)) }
        val extraItems = (firstOffset until secondOffset).map { offset ->
            createLogItem(firstIndex.incrementedBy(offset))
        }

        Segment(firstIndex, baseFolder.toPath(), SIZE_1024).use { segment ->
            items.forEach { item -> segment.append(item) }
        }

        Segment(firstIndex, baseFolder.toPath(), SIZE_1024).load().use { segment ->
            extraItems.forEach { item -> segment.append(item) }
            items.forEach { item -> assertEquals(item, segment.get(item.index)) }
            extraItems.forEach { item -> assertEquals(item, segment.get(item.index)) }
        }
    }

    @Test
    fun `should reject item if segment is full`() {
        val firstIndex = Index(System.nanoTime())
        val item1 = createLogItem(firstIndex)
        val item2 = createLogItem(firstIndex.incremented())
        val singleItemDataSize = LogItemSerializer.HEADER_SIZE_BYTES + item1.getSizeInBytes()

        Segment(firstIndex, baseFolder.toPath(), singleItemDataSize).use { segment ->
            assertTrue(segment.canFit(item1))
            segment.append(item1)
            assertFalse(segment.canFit(item2))
        }
    }

    @Test
    fun `should be able to truncate at a specific index`() {
        val firstIndex = Index(System.nanoTime())
        val firstOffset = Randoms.between(5, 10)
        val secondOffset = Randoms.between(10, 20)
        val items = (0 until firstOffset).map { offset -> createLogItem(firstIndex.incrementedBy(offset)) }
        val tailItems1 = (firstOffset until secondOffset).map { offset ->
            createLogItem(index = firstIndex.incrementedBy(offset))
        }
        val tailItems2 = (firstOffset until secondOffset).map { offset ->
            createLogItem(firstIndex.incrementedBy(offset))
        }

        Segment(firstIndex, baseFolder.toPath(), SIZE_1024).use { segment ->
            items.forEach { item -> segment.append(item) }
            tailItems1.forEach { item -> segment.append(item) }

            val midIndex = tailItems1.first().index
            segment.truncateAt(midIndex)
            tailItems2.forEach { item -> segment.append(item) }

            (items + tailItems2).forEach { item -> assertEquals(item, segment.get(item.index)) }
        }
    }

    @Test
    fun `should be able to delete segments`() {
        val firstIndex = Index(System.nanoTime())
        val firstOffset = Randoms.between(5, 20)
        val items = (0 until firstOffset).map { offset -> createLogItem(firstIndex.incrementedBy(offset)) }

        Segment(firstIndex, baseFolder.toPath(), SIZE_1024).use { segment ->
            items.forEach { item -> segment.append(item) }
        }

        val segment2 = Segment(firstIndex, baseFolder.toPath(), SIZE_1024).load()
        items.forEach { item -> assertEquals(item, segment2.get(item.index)) }
        segment2.delete()

        val segment3 = Segment(firstIndex, baseFolder.toPath(), SIZE_1024).load()
        items.forEach { item -> assertNull(segment3.get(item.index)) }
    }

    @Test
    fun `should properly expose isEmpty method`() {
        val firstIndex = Index(System.nanoTime())
        Segment(firstIndex, baseFolder.toPath(), SIZE_1024).use { segment ->
            assertTrue(segment.isEmpty())
            segment.append(createLogItem(firstIndex))
            assertFalse(segment.isEmpty())
        }
    }

    private fun assertSegmentInitialState(firstIndex: Index, segment: Segment) {
        assertEquals(firstIndex, segment.firstItemIndex)
        assertEquals(firstIndex, segment.lastItemIndex.incremented())
    }

    private fun assertItemStorage(item: LogItem, segment: Segment) {
        assertEquals(item.index, segment.lastItemIndex)
        assertEquals(item, segment.get(item.index))
    }

    private fun createLogItem(index: Index): LogItem {
        return Fixtures.createLogItem(index = index, term = TERM, data = DATA_8_BYTES)
    }

    companion object {

        private const val INDEX_DIFF: Long = 5
        private const val SIZE_1024: Int = 1024
        private val TERM: Term = Term(krandom())
        private val DATA_8_BYTES = Longs.toByteArray(krandom())
    }
}
