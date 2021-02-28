package com.ackbox.raft.types

import kotlin.math.max
import kotlin.math.min

/**
 * Enum representing the state of a node in the cluster.
 */
enum class NodeMode { FOLLOWER, CANDIDATE, LEADER }

/**
 * Data class representing a log item index.
 */
data class Index(val value: Long = UNDEFINED_ID) : Comparable<Index> {
    override operator fun compareTo(other: Index): Int = value.compareTo(other.value)
    operator fun plus(i: Index): Index = copy(value = value + i.value)
    operator fun minus(i: Index): Index = copy(value = value - i.value)

    fun isUndefined(): Boolean = value == UNDEFINED_ID
    fun incremented(): Index = incrementedBy(1)
    fun decremented(): Index = decrementedBy(1)
    fun incrementedBy(increment: Long): Index = Index(value + increment)
    fun decrementedBy(decrement: Long): Index = Index(value - decrement)

    companion object {

        val UNDEFINED: Index = Index(UNDEFINED_ID)
    }
}

/**
 * Data class representing a log item term.
 */
data class Term(val value: Long = UNDEFINED_ID) : Comparable<Term> {
    override operator fun compareTo(other: Term): Int = value.compareTo(other.value)
    operator fun plus(i: Term): Term = copy(value = value + i.value)
    operator fun minus(i: Term): Term = copy(value = value - i.value)

    fun isUndefined(): Boolean = value == UNDEFINED_ID
    fun incremented(): Term = incrementedBy(1)
    fun decremented(): Term = decrementedBy(1)
    fun incrementedBy(increment: Long): Term = Term(value + increment)
    fun decrementedBy(decrement: Long): Term = Term(value - decrement)

    companion object {

        val UNDEFINED: Term = Term(UNDEFINED_ID)
    }
}

/**
 * Data class representing a cluster data partition.
 */
data class Partition(val value: Int) {

    companion object {

        val GLOBAL: Partition = Partition(-1)
    }
}

fun min(first: Index, last: Index): Index = Index(min(first.value, last.value))

fun max(first: Index, last: Index): Index = Index(max(first.value, last.value))

fun max(first: Term, last: Term): Term = Term(max(first.value, last.value))
