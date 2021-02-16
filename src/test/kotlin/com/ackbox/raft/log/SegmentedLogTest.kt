package com.ackbox.raft.log

import com.ackbox.raft.Fixtures
import com.ackbox.raft.config.NodeConfig
import com.ackbox.raft.core.Randoms
import com.ackbox.raft.core.UNDEFINED_ID
import com.ackbox.raft.networking.NodeInmemoryAddress
import com.ackbox.raft.use
import com.ackbox.random.krandom
import com.google.common.primitives.Ints
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File

internal class SegmentedLogTest {

    @TempDir // Needs to be non-private.
    internal lateinit var baseFolder: File

    @Test
    fun `should be able to persist log durably`() {
        val config = createConfig(krandom())
        val firstOffset = Randoms.between(5, 20)
        val items = (0 until firstOffset).map { offset ->
            Fixtures.createLogItem(UNDEFINED_ID + offset)
        }
        SegmentedLog(config).use { log -> log.appendItems(items) }
        SegmentedLog(config).use { log -> items.forEach { item -> assertEquals(item, log.getItem(item.index)) } }
    }

    @Test
    fun `should be able to allocate multiple segments`() {
        val config = createConfig(krandom())
        val expectedSegments = Randoms.between(5, 20).toInt()
        val items = (0 until expectedSegments).map { offset ->
            Fixtures.createLogItem(UNDEFINED_ID + offset, data = Ints.toByteArray(krandom()))
        }
        val segmentedLog = SegmentedLog(config)
        segmentedLog.use { log -> log.appendItems(items) }
        assertEquals(expectedSegments, segmentedLog.getSegmentSize())
    }

    @Test
    fun `should be properly keep track of first and last item indexes`() {
        val config = createConfig(krandom())
        val expectedSegments = Randoms.between(5, 20).toInt()
        val items = (0 until expectedSegments).map { offset ->
            Fixtures.createLogItem(UNDEFINED_ID + offset, data = Ints.toByteArray(krandom()))
        }
        val segmentedLog = SegmentedLog(config)
        segmentedLog.use { log -> log.appendItems(items) }
        assertEquals(items.first().index, segmentedLog.getFirstItemIndex())
        assertEquals(items.last().index, segmentedLog.getLastItemIndex())
    }

    @Test
    fun `should truncate logs if a term mismatch is detected`() {
        val config = createConfig(krandom())
        val term1 = krandom<Long>()
        val term2 = krandom<Long>()
        val prefixOffset = Randoms.between(5, 20)
        val mismatchIndex = Randoms.between(UNDEFINED_ID, prefixOffset)
        val items = (0 until prefixOffset).map { offset -> Fixtures.createLogItem(UNDEFINED_ID + offset, term1) }
        val mismatchItem = Fixtures.createLogItem(mismatchIndex, term2)

        SegmentedLog(config).use { log ->
            log.appendItems(items)
            log.appendItems(listOf(mismatchItem))
        }
        SegmentedLog(config).use { log ->
            (0 until prefixOffset).forEach { index ->
                when {
                    index < mismatchIndex -> assertEquals(items[index.toInt()], log.getItem(index))
                    index == mismatchIndex -> assertEquals(mismatchItem, log.getItem(index))
                    index > mismatchIndex -> assertNull(log.getItem(index))
                }
            }
        }
    }

    @Test
    fun `should delete logs when clear method is called`() {
        val config = createConfig(krandom())
        val items = (0 until Randoms.between(5, 20).toInt()).map { offset ->
            Fixtures.createLogItem(UNDEFINED_ID + offset)
        }
        SegmentedLog(config).use { log -> log.appendItems(items) }
        SegmentedLog(config).use { log -> log.clear() }
        SegmentedLog(config).use { log -> items.forEach { assertNull(log.getItem(it.index)) } }
    }

    @Test
    fun `should be able to truncate before a specific index`() {
        val config = createConfig(krandom())
        val prefixOffset = Randoms.between(25, 50)
        val pivotIndex = Randoms.between(UNDEFINED_ID, prefixOffset)
        val items = (0 until prefixOffset).map { offset -> Fixtures.createLogItem(UNDEFINED_ID + offset) }

        SegmentedLog(config).use { log -> log.appendItems(items) }
        SegmentedLog(config).use { log -> log.truncateBeforeNonInclusive(pivotIndex) }
        SegmentedLog(config).use { log ->
            (0 until prefixOffset).forEach { index ->
                when {
                    index < pivotIndex -> assertNull(log.getItem(index))
                    index >= pivotIndex -> assertEquals(items[index.toInt()], log.getItem(index))
                }
            }
        }
    }

    @Test
    fun `should be able to truncate after a specific index`() {
        val config = createConfig(krandom())
        val prefixOffset = Randoms.between(25, 50)
        val pivotIndex = Randoms.between(UNDEFINED_ID, prefixOffset)
        val items = (0 until prefixOffset).map { offset -> Fixtures.createLogItem(UNDEFINED_ID + offset) }

        SegmentedLog(config).use { log -> log.appendItems(items) }
        SegmentedLog(config).use { log -> log.truncateAfterInclusive(pivotIndex) }
        SegmentedLog(config).use { log ->
            (0 until prefixOffset).forEach { index ->
                when {
                    index < pivotIndex -> assertEquals(items[index.toInt()], log.getItem(index))
                    index >= pivotIndex -> assertNull(log.getItem(index))
                }
            }
        }
    }

    @Test
    fun `should be able to identify whether a log entry matches index and term`() {
        val config = createConfig(krandom())
        val item = Fixtures.createLogItem(UNDEFINED_ID)

        SegmentedLog(config).use { log -> log.appendItems(listOf(item)) }
        SegmentedLog(config).use { log -> assertTrue(log.containsItem(item.index, item.term)) }
    }

    @Test
    fun `should be able to identify whether a log entry matches index and term when log is behind`() {
        val config = createConfig(krandom())
        val item = Fixtures.createLogItem(UNDEFINED_ID)

        SegmentedLog(config).use { log -> assertFalse(log.containsItem(item.index, item.term)) }
    }

    @Test
    fun `should be able to identify whether a log entry matches index and term when term differs`() {
        val config = createConfig(krandom())
        val anotherTerm = krandom<Long>()
        val item = Fixtures.createLogItem(UNDEFINED_ID)

        SegmentedLog(config).use { log -> log.appendItems(listOf(item)) }
        SegmentedLog(config).use { log -> assertFalse(log.containsItem(item.index, anotherTerm)) }
    }

    @Test
    fun `should be able to identify whether the log is ahead for null item`() {
        val config = createConfig(krandom())
        val item = Fixtures.createLogItem(UNDEFINED_ID)

        SegmentedLog(config).use { log -> assertFalse(log.isAheadOf(item.index, item.term)) }
    }

    @Test
    fun `should be able to identify whether the log is ahead for entry with larger term`() {
        val config = createConfig(krandom())
        val item = Fixtures.createLogItem(UNDEFINED_ID)

        SegmentedLog(config).use { log -> log.appendItems(listOf(item)) }
        SegmentedLog(config).use { log -> assertFalse(log.isAheadOf(item.index, item.term + 1)) }
    }

    @Test
    fun `should be able to identify whether the log is ahead for entry with smaller term`() {
        val config = createConfig(krandom())
        val item = Fixtures.createLogItem(UNDEFINED_ID)

        SegmentedLog(config).use { log -> log.appendItems(listOf(item)) }
        SegmentedLog(config).use { log -> assertTrue(log.isAheadOf(item.index, item.term - 1)) }
    }

    @Test
    fun `should be able to identify whether the log is ahead for entry with greater index`() {
        val config = createConfig(krandom())
        val item = Fixtures.createLogItem(UNDEFINED_ID)

        SegmentedLog(config).use { log -> log.appendItems(listOf(item)) }
        SegmentedLog(config).use { log -> assertFalse(log.isAheadOf(item.index + 1, item.term)) }
    }

    @Test
    fun `should be able to identify whether the log is ahead for entry with smaller index`() {
        val config = createConfig(krandom())
        val item = Fixtures.createLogItem(UNDEFINED_ID)

        SegmentedLog(config).use { log -> log.appendItems(listOf(item)) }
        SegmentedLog(config).use { log -> assertTrue(log.isAheadOf(item.index - 1, item.term)) }
    }

    private fun createConfig(nodeId: String): NodeConfig {
        val local = NodeInmemoryAddress(nodeId)
        val remote = NodeInmemoryAddress(krandom())
        return NodeConfig(
            local = local,
            remotes = listOf(remote),
            dataBaseFolder = baseFolder.absolutePath,
            maxLogSegmentSizeInBytes = Int.SIZE_BYTES
        )
    }
}
