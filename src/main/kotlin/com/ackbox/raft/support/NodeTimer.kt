package com.ackbox.raft.support

import com.ackbox.raft.config.NodeConfig
import com.ackbox.raft.types.Partition
import java.util.Timer
import kotlin.concurrent.scheduleAtFixedRate

typealias Callback = () -> Unit

class NodeTimer(private val config: NodeConfig, partition: Partition) {

    private val logger: NodeLogger = NodeLogger.forPartition(config.nodeId, partition, NodeTimer::class)
    private var electionTimer: Timer = createElectionTimer()
    private var heartbeatTimer: Timer = createHeartbeatTimer()
    private var snapshotTimer: Timer = createSnapshotTimer()

    fun restartElectionTimer(electionCallback: Callback?) {
        stopElectionTimer()
        val delay = config.electionDelay
        logger.debug("Setting up election timer to [{}] ms", delay.toMillis())
        electionTimer = createElectionTimer()
        electionTimer.scheduleAtFixedRate(delay.toMillis(), delay.toMillis()) { safely(electionCallback) }
    }

    fun restartHeartbeatTimer(heartbeatCallback: Callback?) {
        stopHeartbeatTimer()
        val delay = config.heartbeatDelay
        logger.debug("Setting up heartbeat timer to [{}] ms", delay.toMillis())
        heartbeatTimer = createHeartbeatTimer()
        heartbeatTimer.scheduleAtFixedRate(delay.toMillis(), delay.toMillis()) { safely(heartbeatCallback) }
    }

    fun restartSnapshotTimer(snapshotCallback: Callback?) {
        stopSnapshotTimer()
        val delay = config.snapshotDelay
        logger.debug("Setting up snapshot timer to [{}] minutes", delay.toMinutes())
        snapshotTimer = createSnapshotTimer()
        snapshotTimer.scheduleAtFixedRate(delay.toMillis(), delay.toMillis()) { safely(snapshotCallback) }
    }

    fun stopAll() {
        stopElectionTimer()
        stopHeartbeatTimer()
        stopSnapshotTimer()
    }

    fun stopElectionTimer() {
        electionTimer.cancel()
        electionTimer.purge()
    }

    fun stopHeartbeatTimer() {
        heartbeatTimer.cancel()
        heartbeatTimer.purge()
    }

    private fun stopSnapshotTimer() {
        snapshotTimer.cancel()
        snapshotTimer.purge()
    }

    private fun createElectionTimer(): Timer {
        return Timer("ElectionTimer")
    }

    private fun createHeartbeatTimer(): Timer {
        return Timer("HeartbeatTimer")
    }

    private fun createSnapshotTimer(): Timer {
        return Timer("SnapshotTimer")
    }

    private fun safely(callback: Callback?) {
        try {
            callback?.invoke()
        } catch (e: Exception) {
            logger.warn("Caught exception while executing callback", e)
        }
    }
}
