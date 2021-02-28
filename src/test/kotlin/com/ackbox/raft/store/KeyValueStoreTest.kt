package com.ackbox.raft.store

import com.ackbox.raft.Fixtures
import com.ackbox.random.krandom
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.nio.file.Paths

internal class KeyValueStoreTest {

    @TempDir // Needs to be non-private.
    internal lateinit var baseFolder: File

    @Test
    fun `should be able to retrieved applied value`() {
        val config = Fixtures.createNodeConfig()
        val store = KeyValueStore(config)

        val key = krandom<String>()
        val entry1 = KV(key, krandom<String>().toByteArray())
        val entry2 = KV(key, krandom<String>().toByteArray())

        store.applyValue(entry1.toByteArray())
        val value1 = store.getValue(key)

        store.applyValue(entry2.toByteArray())
        val value2 = store.getValue(key)

        assertArrayEquals(entry1.toByteArray(), value1)
        assertArrayEquals(entry2.toByteArray(), value2)
    }

    @Test
    fun `should be able to recover state from snapshot`() {
        val config = Fixtures.createNodeConfig()
        val store1 = KeyValueStore(config)
        val store2 = KeyValueStore(config)
        val entries = generateEntries()

        entries.forEach { store1.applyValue(it.toByteArray()) }

        val destinationPath = Paths.get(baseFolder.absolutePath)
        store1.takeSnapshot(destinationPath)
        store2.restoreSnapshot(destinationPath)

        entries.forEach { assertArrayEquals(it.toByteArray(), store2.getValue(it.key)) }
    }

    @Test
    fun `should not fail if snapshot file does not exist`() {
        val config = Fixtures.createNodeConfig()
        val store = KeyValueStore(config)
        val entries = generateEntries()

        val destinationPath = Paths.get(baseFolder.absolutePath)
        entries.forEach { store.applyValue(it.toByteArray()) }

        store.restoreSnapshot(destinationPath)

        entries.forEach { assertArrayEquals(it.toByteArray(), store.getValue(it.key)) }
    }

    private fun generateEntries(): List<KV> {
        return (0..50).map { KV(krandom(), krandom<String>().toByteArray()) }
    }
}
