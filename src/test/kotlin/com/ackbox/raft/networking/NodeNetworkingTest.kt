package com.ackbox.raft.networking

import com.ackbox.raft.Fixtures
import com.ackbox.raft.config.NodeConfig
import com.ackbox.raft.networking.NodeNetworkingChange.Type.ADDED
import com.ackbox.raft.networking.NodeNetworkingChange.Type.REMOVED
import com.ackbox.random.krandom
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.nio.file.Paths

internal class NodeNetworkingTest {

    @TempDir // Needs to be non-private.
    internal lateinit var baseFolder: File

    @Test
    fun `should create networking with channels from config`() {
        val config = createNodeConfig(NodeInmemoryAddress(REMOTE_NODE_ID1), NodeInmemoryAddress(REMOTE_NODE_ID2))
        val networking = NodeNetworking(config)
        val expectedAddresses = listOf(
            NodeInmemoryAddress(REMOTE_NODE_ID1),
            NodeInmemoryAddress(REMOTE_NODE_ID2)
        )

        assertEquals(expectedAddresses.sorted(), networking.getChannels().map { it.address }.sorted())
    }

    @Test
    fun `should notify callback when a channel is added`() {
        val config = createNodeConfig(NodeInmemoryAddress(REMOTE_NODE_ID1), NodeInmemoryAddress(REMOTE_NODE_ID2))
        val networking = NodeNetworking(config)
        val modifiedAddresses = mutableListOf<NodeAddress>()
        val addedAddress = NodeInmemoryAddress(REMOTE_NODE_ID3)
        val expectedAddresses = listOf(
            NodeInmemoryAddress(REMOTE_NODE_ID1),
            NodeInmemoryAddress(REMOTE_NODE_ID2),
            NodeInmemoryAddress(REMOTE_NODE_ID3)
        )

        networking.setOnChannelAdded { modifiedAddresses.add(it.address) }
        networking.applyValue(NodeNetworkingChange(ADDED, addedAddress).toByteArray())

        assertEquals(addedAddress, modifiedAddresses.single())
        assertEquals(expectedAddresses.sorted(), networking.getChannels().map { it.address }.sorted())
    }

    @Test
    fun `should notify callback when a channel is removed`() {
        val config = createNodeConfig(NodeInmemoryAddress(REMOTE_NODE_ID1), NodeInmemoryAddress(REMOTE_NODE_ID2))
        val networking = NodeNetworking(config)
        val modifiedAddresses = mutableListOf<NodeAddress>()
        val removedAddress = NodeInmemoryAddress(REMOTE_NODE_ID2)
        val expectedAddresses = listOf(NodeInmemoryAddress(REMOTE_NODE_ID1))

        networking.setOnChannelRemoved { modifiedAddresses.add(it.address) }
        networking.applyValue(NodeNetworkingChange(REMOVED, removedAddress).toByteArray())

        assertEquals(removedAddress, modifiedAddresses.single())
        assertEquals(expectedAddresses.sorted(), networking.getChannels().map { it.address }.sorted())
    }

    @Test
    fun `should be able to take and restore snapshot`() {
        val config1 = createNodeConfig(NodeInmemoryAddress(REMOTE_NODE_ID1), NodeInmemoryAddress(REMOTE_NODE_ID2))
        val networking1 = NodeNetworking(config1)

        val config2 = createNodeConfig()
        val networking2 = NodeNetworking(config2)

        val destinationPath = Paths.get(baseFolder.absolutePath)
        networking1.takeSnapshot(destinationPath)
        networking2.restoreSnapshot(destinationPath)

        assertEquals(
            networking1.getChannels().map { it.address }.sorted(),
            networking2.getChannels().map { it.address }.sorted()
        )
    }

    @Test
    fun `should override any previous state with snapshot`() {
        val remotes1 = listOf(NodeInmemoryAddress(REMOTE_NODE_ID1), NodeInmemoryAddress(REMOTE_NODE_ID2))
        val config1 = createNodeConfig(*remotes1.toTypedArray())
        val networking1 = NodeNetworking(config1)

        val remotes2 = listOf(NodeInmemoryAddress(REMOTE_NODE_ID3), NodeInmemoryAddress(UNKNOWN_NODE_ID))
        val config2 = createNodeConfig(*remotes2.toTypedArray())
        val networking2 = NodeNetworking(config2)

        val removedAddresses = mutableListOf<NodeAddress>()
        val addedAddresses = mutableListOf<NodeAddress>()

        val destinationPath = Paths.get(baseFolder.absolutePath)
        networking1.takeSnapshot(destinationPath)
        networking2.setOnChannelRemoved { removedAddresses.add(it.address) }
        networking2.setOnChannelAdded { addedAddresses.add(it.address) }
        networking2.restoreSnapshot(destinationPath)

        assertEquals(addedAddresses.sorted(), remotes1.sorted())
        assertEquals(removedAddresses.sorted(), remotes2.sorted())
        assertEquals(
            networking1.getChannels().map { it.address }.sorted(),
            networking2.getChannels().map { it.address }.sorted()
        )
    }

    @Test
    fun `should not fail if snapshot file does not exist`() {
        val remotes = listOf(NodeInmemoryAddress(REMOTE_NODE_ID3), NodeInmemoryAddress(UNKNOWN_NODE_ID))
        val config = createNodeConfig(*remotes.toTypedArray())
        val networking = NodeNetworking(config)

        val destinationPath = Paths.get(baseFolder.absolutePath)
        networking.restoreSnapshot(destinationPath)

        assertEquals(remotes.sorted(), networking.getChannels().map { it.address }.sorted())
    }

    private fun createNodeConfig(vararg remotes: NodeAddress): NodeConfig {
        return Fixtures.createNodeConfig(*remotes)
    }

    companion object {

        private val REMOTE_NODE_ID1: String = krandom()
        private val REMOTE_NODE_ID2: String = krandom()
        private val REMOTE_NODE_ID3: String = krandom()
        private val UNKNOWN_NODE_ID: String = krandom()
    }
}
