package com.ackbox.raft.core

const val UNDEFINED_ID: Long = 0
const val MIN_ELECTION_TIMER_MS: Long = 3 * 1000
const val MAX_ELECTION_TIMER_MS: Long = 6 * 1000
const val HEARTBEAT_DELAY_RATIO: Int = 3

const val START_NODE_OPERATION = "StartNodeOperation"
const val STOP_NODE_OPERATION = "StopNodeOperation"
const val HANDLE_APPEND_OPERATION = "HandleAppendOperation"
const val HANDLE_VOTE_OPERATION = "HandleVoteOperation"
const val REQUEST_VOTE_OPERATION = "RequestVoteOperation"
const val REQUEST_APPEND_OPERATION = "RequestAppendOperation"
const val REQUEST_RETRIEVE_OPERATION = "RequestRetrieveOperation"

object Randoms {

    fun between(lower: Long, upper: Long): Long = ((Math.random() * (upper - lower)) + lower).toLong()
}
