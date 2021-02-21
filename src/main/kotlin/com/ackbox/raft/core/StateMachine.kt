package com.ackbox.raft.core

import java.nio.file.Path

/**
 * Interface for Raft's replicated state machine.
 */
interface StateMachine {

    /**
     * Apply a state value to the state machine. Each entry contains command for state machine, and term when
     * entry was received by leader.
     */
    fun applyValue(value: ByteArray)

    /**
     * Take a snapshot of the state machine's state. The resulting snapshot should be consistent. The
     * algorithm implementation will ensure that any mutation operation is blocked until this method
     * returns. This means that the implementation of this method does not need to be concerned about
     * concurrency issues.
     */
    fun takeSnapshot(destinationPath: Path)

    /**
     * Load a snapshot of the state machine's state. The snapshot loading is atomic, meaning that the
     * entire state machine's state will have been replaced by the snapshot provided once the method
     * returns. The algorithm implementation will ensure that any mutation operation is blocked until
     * this method returns. This means that the implementation of this method does not need to be
     * concerned about concurrency issues.
     */
    fun restoreSnapshot(sourcePath: Path)
}
