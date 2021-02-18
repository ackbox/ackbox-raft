package com.ackbox.raft.types

import java.nio.file.Paths
import java.time.Duration

val SNAPSHOT_DELAY: Duration = Duration.ofMinutes(1)
val STATE_LOCK_WAIT_TIMEOUT: Duration = Duration.ofSeconds(2)
val REMOTE_TIMEOUT: Duration = Duration.ofSeconds(5)

val WORKING_FOLDER: String = Paths.get(".").toAbsolutePath().normalize().toString()
val DATA_BASE_FOLDER: String = Paths.get(WORKING_FOLDER, "data").toAbsolutePath().toString()
const val LOG_SEGMENT_SIZE_IN_BYTES: Int = 1024

const val MIN_ELECTION_TIMER_MS: Long = 3 * 1000
const val MAX_ELECTION_TIMER_MS: Long = 6 * 1000
const val HEARTBEAT_DELAY_RATIO: Int = 3

const val UNDEFINED_ID: Long = 0

const val DESCRIBE_STATE_OPERATION: String = "DescribeStateOperation"
const val START_NODE_OPERATION: String = "StartNodeOperation"
const val STOP_NODE_OPERATION: String = "StopNodeOperation"
const val HANDLE_APPEND_OPERATION: String = "HandleAppendOperation"
const val HANDLE_VOTE_OPERATION: String = "HandleVoteOperation"
const val HANDLE_SNAPSHOT_OPERATION: String = "HandleSnapshotOperation"
const val REQUEST_VOTE_OPERATION: String = "RequestVoteOperation"
const val REQUEST_APPEND_OPERATION: String = "RequestAppendOperation"
const val REQUEST_RETRIEVE_OPERATION: String = "RequestRetrieveOperation"
const val REQUEST_SNAPSHOT_OPERATION: String = "RequestSnapshotOperation"

object Randoms {

    fun between(lower: Long, upper: Long): Long = ((Math.random() * (upper - lower)) + lower).toLong()
}
