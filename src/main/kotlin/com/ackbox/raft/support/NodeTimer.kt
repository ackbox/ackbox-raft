package com.ackbox.raft.support

import com.ackbox.raft.config.NodeConfig
import java.util.Timer
import kotlin.concurrent.scheduleAtFixedRate

typealias Callback = () -> Unit

class NodeTimer(private val config: NodeConfig) {

    private val logger: NodeLogger = NodeLogger.from(config.nodeId, NodeTimer::class)
    private var electionTimer: Timer = createElectionTimer()
    private var heartbeatTimer: Timer = createHeartbeatTimer()

    fun restartElectionTimer(electionCallback: Callback?) {
        stopElectionTimer()
        val delay = config.getElectionDelay()
        logger.debug("Setting up election timer to [{}] ms", delay.toMillis())
        electionTimer = createElectionTimer()
        electionTimer.scheduleAtFixedRate(delay.toMillis(), delay.toMillis()) { safely(electionCallback) }
    }

    fun restartHeartbeatTimer(heartbeatCallback: Callback?) {
        stopHeartbeatTimer()
        val delay = config.getHeartbeatDelay()
        logger.debug("Setting up heartbeat timer to [{}] ms", delay.toMillis())
        heartbeatTimer = createHeartbeatTimer()
        heartbeatTimer.scheduleAtFixedRate(delay.toMillis(), delay.toMillis()) { safely(heartbeatCallback) }
    }

    fun stopElectionTimer() {
        electionTimer.cancel()
        electionTimer.purge()
    }

    fun stopHeartbeatTimer() {
        heartbeatTimer.cancel()
        heartbeatTimer.purge()
    }

    private fun createElectionTimer(): Timer {
        return Timer("ElectionTimer::${config.nodeId}")
    }

    private fun createHeartbeatTimer(): Timer {
        return Timer("HeartbeatTimer::${config.nodeId}")
    }

    private fun safely(callback: Callback?) {
        try {
            callback?.invoke()
        } catch (e: Exception) {
            logger.warn("Caught exception while executing callback", e)
        }
    }
}
