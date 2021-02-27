package simple

import com.ackbox.raft.Raft
import com.ackbox.raft.api.ExternalNodeGrpc
import com.ackbox.raft.api.SetEntryRequest
import com.ackbox.raft.config.NodeConfig
import com.ackbox.raft.networking.NodeNetAddress
import com.ackbox.raft.store.KV
import com.google.protobuf.ByteString
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import java.nio.ByteBuffer
import java.time.Duration
import java.util.UUID

object Configuration {

    private val nodeAddress1 = NodeNetAddress("1", "localhost", 50051)
    private val nodeAddress2 = NodeNetAddress("2", "localhost", 50052)
    private val nodeAddress3 = NodeNetAddress("3", "localhost", 50053)

    val node1Config
        get() = NodeConfig.newConfig(nodeAddress1, nodeAddress1, nodeAddress2, nodeAddress3)
            .copy(snapshotDelay = Duration.ofSeconds(10))

    val node2Config get() = NodeConfig.newConfig(nodeAddress2, nodeAddress1, nodeAddress2, nodeAddress3)

    val node3Config get() = NodeConfig.newConfig(nodeAddress3, nodeAddress1, nodeAddress2, nodeAddress3)
}

class LoopNode(private val config: NodeConfig) {

    private val api = ExternalNodeGrpc.newBlockingStub(config.local.toChannel())

    fun run() {
        val node1 = Raft.fromConfig(config)
        scheduleSendEntries(config)
        node1.describeState()
        node1.start().autoStop().join()
    }

    private fun scheduleSendEntries(config: NodeConfig) {
        GlobalScope.launch {
            delay(COOLDOWN.toMillis())
            while (isActive) {
                println("Sending SET request")
                val request = createRequest()
                val reply = api.runCatching { setEntry(request) }
                if (config.nodeId != reply.getOrNull()?.leaderId) {
                    delay(COOLDOWN.toMillis())
                }
                delay(DELAY.toMillis())
            }
        }
    }

    private fun createRequest(): SetEntryRequest {
        val key = UUID.randomUUID().toString()
        val data = ByteBuffer.wrap(UUID.randomUUID().toString().toByteArray())
        return SetEntryRequest.newBuilder()
            .setTimestamp(System.currentTimeMillis())
            .setEntry(ByteString.copyFrom(KV(key, data).toByteArray()))
            .build()
    }

    companion object {

        private val COOLDOWN = Duration.ofSeconds(15)
        private val DELAY = Duration.ofSeconds(2)
    }
}
