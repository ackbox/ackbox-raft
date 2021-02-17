package com.ackbox.raft.statemachine

import com.ackbox.raft.config.NodeConfig
import com.ackbox.raft.support.NodeLogger
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import java.io.File
import java.nio.ByteBuffer
import java.nio.file.Path
import javax.annotation.concurrent.NotThreadSafe

@NotThreadSafe
class KeyValueStore(config: NodeConfig) : ReplicatedStateMachine {

    private val logger: NodeLogger = NodeLogger.from(config.nodeId, KeyValueStore::class)
    private val store: MutableMap<String, KV> = mutableMapOf()

    override fun setValue(value: ByteArray) {
        logger.info("Setting item [{}]", value)
        val kv = SERIALIZER.readValue<KV>(value)
        store[kv.key] = kv
    }

    override fun getValue(key: String): ByteArray? {
        logger.info("Getting item with key [{}]", key)
        return store[key]?.let { kv -> SERIALIZER.writeValueAsBytes(kv) }
    }

    override fun takeSnapshot(): Path {
        logger.info("Taking a snapshot of [{}] entries", store.size)
        val file = File.createTempFile("$SNAPSHOT_PREFIX-${System.currentTimeMillis()}", SNAPSHOT_EXT)
        SERIALIZER.writeValue(file, store)
        return file.toPath()
    }

    override fun restoreSnapshot(dataPath: Path) {
        store.clear()
        store.putAll(SERIALIZER.readValue<Map<String, KV>>(dataPath.toFile()))
    }

    companion object {

        private val SERIALIZER: ObjectMapper = ObjectMapper().registerKotlinModule()
        private val SNAPSHOT_PREFIX: String = "snapshot"
        private val SNAPSHOT_EXT: String = "snapshot.temp"
    }
}

/**
 * Kay-value representation of log data.
 */
data class KV(val key: String, val value: ByteBuffer)
