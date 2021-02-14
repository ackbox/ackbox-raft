package com.ackbox.raft.config

import com.ackbox.raft.core.HEARTBEAT_DELAY_RATIO
import com.ackbox.raft.core.LOG_BASE_FOLDER
import com.ackbox.raft.core.LOG_SEGMENT_SIZE_IN_BYTES
import com.ackbox.raft.core.MAX_ELECTION_TIMER_MS
import com.ackbox.raft.core.MIN_ELECTION_TIMER_MS
import com.ackbox.raft.core.REMOTE_TIMEOUT
import com.ackbox.raft.core.Randoms
import com.ackbox.raft.networking.NodeAddress
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Clock
import java.time.Duration

/**
 * Configuration for nodes in the cluster.
 */
data class NodeConfig(
    /**
     * The [NodeAddress] of the node running locally (also referred as "local node").
     */
    val local: NodeAddress,

    /**
     * The [NodeAddress]s of other nodes composing the cluster (also referred as "peer/remote nodes").
     */
    val remotes: List<NodeAddress>,

    /**
     * Implementation of [Clock] what should be used by the local node.
     * It defaults to [Clock.systemUTC()].
     */
    val clock: Clock = Clock.systemUTC(),

    /**
     * By default, the node's log is divided by segments. The size of these segments is configurable.
     * This parameter sets the size (in bytes) of these segments.
     * It defaults to [LOG_SEGMENT_SIZE_IN_BYTES].
     */
    val maxLogSegmentSizeInBytes: Int = LOG_SEGMENT_SIZE_IN_BYTES,

    /**
     * Base path to the folder where the log should be stored.
     * It defaults to [LOG_BASE_FOLDER].
     */
    private val logBaseFolder: String = LOG_BASE_FOLDER,

    /**
     * Maximum wait time for responses from remote nodes when performing RPC requests.
     * It defaults to [REMOTE_TIMEOUT].
     */
    private val remoteRpcTimeout: Duration = REMOTE_TIMEOUT,

    /**
     * The election timeout is randomly chosen from minimum and maximum values. This parameter
     * sets the minimum and maximum election timeouts that will be used by the algorithm.
     * It defaults to [MIN_ELECTION_TIMER_MS] and [MAX_ELECTION_TIMER_MS].
     */
    private val electionDelayRangeMs: Pair<Long, Long> = (MIN_ELECTION_TIMER_MS to MAX_ELECTION_TIMER_MS),

    /**
     * The leader heartbeat is defined as a ration of [min(electionDelayRangeMs)]. Typically, we would
     * like to heartbeat period to be lower than the election timeout in order to avoid unnecessary
     * leadership changes due to quick network events.
     * It defaults to [HEARTBEAT_DELAY_RATIO].
     */
    private val heartbeatDelayRatio: Int = HEARTBEAT_DELAY_RATIO,
) {

    /**
     * Return the unique identifier for this node.
     */
    val nodeId: String = local.nodeId

    /**
     * Return log path for this node on the file system.
     */
    val logPath: Path get() = Paths.get(logBaseFolder, nodeId)

    /**
     * Return maximum wait time for responses from remote nodes when performing RPC requests.
     */
    val remoteRpcTimeoutDuration: Duration get() = remoteRpcTimeout

    /**
     * Compute a random value in the range [electionDelayRangeMs] for the election timeout of the node.
     */
    val electionDelay: Duration
        get() = Duration.ofMillis(Randoms.between(electionDelayRangeMs.first, electionDelayRangeMs.second))

    /**
     * Return the heartbeat period/delay for this node.
     */
    val heartbeatDelay: Duration
        get() = Duration.ofMillis(electionDelayRangeMs.first / heartbeatDelayRatio)

    companion object {

        fun newConfig(local: NodeAddress, vararg targets: NodeAddress): NodeConfig = NodeConfig(local, targets.toList())
    }
}
