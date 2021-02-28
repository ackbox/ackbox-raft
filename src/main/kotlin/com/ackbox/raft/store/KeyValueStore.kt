package com.ackbox.raft.store

import com.ackbox.raft.config.NodeConfig
import com.ackbox.raft.core.StateMachine
import com.ackbox.raft.support.NodeLogger
import com.ackbox.raft.support.SERIALIZER
import com.fasterxml.jackson.module.kotlin.readValue
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.ConcurrentHashMap
import javax.annotation.concurrent.ThreadSafe

@ThreadSafe
class KeyValueStore(config: NodeConfig) : StateMachine {

    private val logger: NodeLogger = NodeLogger.forNode(config.nodeId, KeyValueStore::class)
    private val store: MutableMap<String, KV> = ConcurrentHashMap()

    fun getValue(key: String): ByteArray? {
        logger.info("Getting item with key [{}]", key)
        return store[key]?.toByteArray()
    }

    override fun applyValue(value: ByteArray) {
        logger.info("Setting item [{}]", value)
        val kv = KV.fromByteArray(value)
        store[kv.key] = kv
    }

    override fun takeSnapshot(destinationPath: Path) {
        logger.info("Taking a snapshot of [{}] entries and saving to [{}]", store.size, destinationPath)
        val filePath = Paths.get(destinationPath.toAbsolutePath().toString(), SNAPSHOT_FILENAME)
        synchronized(store) { SERIALIZER.writeValue(filePath.toFile(), SerializerWrapper(store)) }
    }

    override fun restoreSnapshot(sourcePath: Path) {
        logger.info("Restoring a snapshot from [{}]", sourcePath)
        val file = Paths.get(sourcePath.toAbsolutePath().toString(), SNAPSHOT_FILENAME).toFile()
        if (!file.exists()) {
            return
        }
        synchronized(store) {
            store.clear()
            store.putAll(SERIALIZER.readValue<SerializerWrapper>(file).data)
        }
    }

    internal data class SerializerWrapper(val data: Map<String, KV>)

    companion object {

        private const val SNAPSHOT_FILENAME: String = "store.data.snapshot"

        fun fromConfig(config: NodeConfig): KeyValueStore = KeyValueStore(config)
    }
}
