package com.ackbox.raft.config

import com.ackbox.raft.core.HEARTBEAT_DELAY_RATIO
import com.ackbox.raft.core.MAX_ELECTION_TIMER_MS
import com.ackbox.raft.core.MIN_ELECTION_TIMER_MS
import com.ackbox.raft.core.Randoms
import com.ackbox.raft.networking.NodeAddress
import java.time.Clock
import java.time.Duration

data class NodeConfig(
    val local: NodeAddress,
    val targets: List<NodeAddress>,
    val clock: Clock = Clock.systemUTC(),
    private val electionDelayRangeMs: Pair<Long, Long> = (MIN_ELECTION_TIMER_MS to MAX_ELECTION_TIMER_MS),
    private val heartbeatDelayRatio: Int = HEARTBEAT_DELAY_RATIO,
) {

    val nodeId: String = local.nodeId

    fun getElectionDelay(): Duration {
        return Duration.ofMillis(Randoms.between(electionDelayRangeMs.first, electionDelayRangeMs.second))
    }

    fun getHeartbeatDelay(): Duration {
        return Duration.ofMillis(electionDelayRangeMs.first / heartbeatDelayRatio)
    }

    companion object {

        fun newConfig(local: NodeAddress, vararg targets: NodeAddress): NodeConfig = NodeConfig(local, targets.toList())
    }
}
