package com.ackbox.raft.support

/**
 * Exception for the case where current term does not match the term from a remote peer/node.
 */
class ReplyTermInvariantException(val currentTerm: Long, val remoteTerm: Long) :
    RetryableException("Term mismatch: current=[$currentTerm] and remote=[$remoteTerm]")

/**
 * Exception for the case where current term does not match the term from leader node.
 */
class RequestTermInvariantException(val term: Long, remoteTerm: Long, val lastLogIndex: Long) :
    RetryableException("Term mismatch: current=[$term], remote=[$remoteTerm] and lastLogIndex=[$lastLogIndex]")

/**
 * Exception for the case where a node receive a request from a leader that it does not recognize.
 */
class LeaderMismatchException(knownLeaderId: String?, val term: Long, val lastLogIndex: Long) :
    RetryableException("Node is not the leader - known leaderId=[$knownLeaderId]")

/**
 * Exception for the case where a replica node does not agree with leader node's state.
 */
class ReplicaStateMismatchException(val term: Long, val lastLogIndex: Long) :
    RetryableException("Inconsistent state: term=[$term] and lastLogIndex=[$lastLogIndex]")

/**
 * Exception for the case where a replica node is not able to vote for the candidate.
 */
class VoteNotGrantedException(candidateId: String, val term: Long) :
    NonRetryableException("Replica cannot vote for candidate: candidate=[$candidateId] and term=[$term]")

/**
 * Exception for the case where a node receive a request that can only be handled by the leader,
 * but its mode is follower or candidate.
 */
class NotLeaderException(val knownLeaderId: String?) :
    RetryableException("Node is not the leader - known leaderId=[$knownLeaderId]")

/**
 * Exception for the case where followers are not able to agree on the desired replication state.
 */
class CommitIndexMismatchException(val leaderId: String?, desiredCommitIndex: Long, actualCommitIndex: Long) :
    NonRetryableException("Unable to reach consensus on commit index - desired=[$desiredCommitIndex] and actual=[$actualCommitIndex]")

/**
 * Exception for the case where the node was unable to acquire the lock in order to safely modify its state.
 * It is safe for requests to be retried upon reception of this exception.
 */
class LockNotAcquiredException : RetryableException("Unable to acquire state lock")

/**
 * Exception for transient failures. Callers can retry upon receiving this exception.
 */
abstract class RetryableException(message: String?, cause: Throwable? = null) : RuntimeException(message, cause)

/**
 * Exception for terminal failures. Callers should NOT retry upon receiving this exception.
 */
abstract class NonRetryableException(message: String?, cause: Throwable? = null) : RuntimeException(message, cause)
