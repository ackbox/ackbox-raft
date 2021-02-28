package com.ackbox.raft.config

import com.ackbox.raft.networking.NodeAddress
import com.ackbox.raft.types.DATA_BASE_FOLDER
import com.ackbox.raft.types.HEARTBEAT_DELAY_RATIO
import com.ackbox.raft.types.LOG_SEGMENT_SIZE_IN_BYTES
import com.ackbox.raft.types.MAX_ELECTION_TIMER_MS
import com.ackbox.raft.types.MIN_ELECTION_TIMER_MS
import com.ackbox.raft.types.PARTITION_COUNT
import com.ackbox.raft.types.Partition
import com.ackbox.raft.types.REMOTE_TIMEOUT
import com.ackbox.raft.types.Randoms
import com.ackbox.raft.types.SNAPSHOT_DELAY
import com.ackbox.raft.types.STATE_LOCK_WAIT_TIMEOUT
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
     * The data in the cluster is split up across partition in order to improve throughput.
     * * It defaults to [PARTITION_COUNT].
     */
    val partitionCount: Int = PARTITION_COUNT,

    /**
     * By default, the node's log is divided by segments. The size of these segments is configurable.
     * This parameter sets the size (in bytes) of these segments.
     * It defaults to [LOG_SEGMENT_SIZE_IN_BYTES].
     */
    val maxLogSegmentSizeInBytes: Int = LOG_SEGMENT_SIZE_IN_BYTES,

    /**
     * Maximum amount of time that requests will be waiting to acquire the state lock for this node.
     * It defaults to [STATE_LOCK_WAIT_TIMEOUT].
     */
    val maxStateLockWaitTimeout: Duration = STATE_LOCK_WAIT_TIMEOUT,

    /**
     * The snapshot delay defines how frequently a node will take a snapshot if its state and trim its log.
     * It defaults to [SNAPSHOT_DELAY].
     */
    val snapshotDelay: Duration = SNAPSHOT_DELAY,

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

    /**
     * Base path to the folder where the all node data should be stored.
     * It defaults to [DATA_BASE_FOLDER].
     */
    private val dataBaseFolder: String = DATA_BASE_FOLDER,

    /**
     * Maximum wait time for responses from remote nodes when performing RPC requests.
     * It defaults to [REMOTE_TIMEOUT].
     */
    private val remoteRpcTimeout: Duration = REMOTE_TIMEOUT
) {

    /**
     * Return the unique identifier for this node.
     */
    val nodeId: String = local.nodeId

    /**
     * Returns all enabled partitions in the cluster.
     */
    val partitions = (1..partitionCount).asSequence().map { Partition(it) }

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

    /**
     * Return log path for this node on the file system.
     */
    fun getLogPath(partition: Partition): Path {
        return Paths.get(dataBaseFolder, nodeId, partitionId(partition), "log")
    }

    /**
     * Return snapshot path for this node on the file system.
     */
    fun getSnapshotPath(partition: Partition): Path {
        return Paths.get(dataBaseFolder, nodeId, partitionId(partition), "snapshot")
    }

    private fun partitionId(partition: Partition): String = "partition-${partition.value}"

    companion object {

        fun newConfig(local: NodeAddress, vararg targets: NodeAddress): NodeConfig = NodeConfig(local, targets.toList())
    }
}
