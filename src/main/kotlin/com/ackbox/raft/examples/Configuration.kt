package com.ackbox.raft.examples

import com.ackbox.raft.PublicNodeGrpc
import com.ackbox.raft.Raft
import com.ackbox.raft.SetRequest
import com.ackbox.raft.config.NodeConfig
import com.ackbox.raft.networking.NodeNetAddress
import com.google.protobuf.ByteString
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import java.time.Duration
import java.util.UUID

object Configuration {

    private val nodeAddress1 = NodeNetAddress("1", "localhost", 50051)
    private val nodeAddress2 = NodeNetAddress("2", "localhost", 50052)
    private val nodeAddress3 = NodeNetAddress("3", "localhost", 50053)

    val node1Config get() = NodeConfig.newConfig(nodeAddress1, nodeAddress2, nodeAddress3)

    val node2Config get() = NodeConfig.newConfig(nodeAddress2, nodeAddress1, nodeAddress3)

    val node3Config get() = NodeConfig.newConfig(nodeAddress3, nodeAddress2, nodeAddress1)
}

class LoopNode(private val config: NodeConfig) {

    private val api = PublicNodeGrpc.newBlockingStub(config.local.toChannel())

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
                val reply = api.runCatching { set(request) }
                if (config.nodeId != reply.getOrNull()?.leaderId) {
                    delay(COOLDOWN.toMillis())
                }
                delay(DELAY.toMillis())
            }
        }
    }

    private fun createRequest(): SetRequest {
        return SetRequest.newBuilder()
            .setTimestamp(System.currentTimeMillis())
            .setEntry(ByteString.copyFrom(UUID.randomUUID().toString().toByteArray()))
            .build()
    }

    companion object {

        private val COOLDOWN = Duration.ofSeconds(15)
        private val DELAY = Duration.ofSeconds(2)
    }
}
