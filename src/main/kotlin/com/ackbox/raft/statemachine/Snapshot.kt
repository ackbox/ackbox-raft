package com.ackbox.raft.statemachine

import com.ackbox.raft.state.Index
import com.ackbox.raft.state.Term
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.io.path.ExperimentalPathApi
import kotlin.io.path.createDirectories
import kotlin.io.path.deleteIfExists
import kotlin.io.path.outputStream
import kotlin.io.path.readBytes

/**
 * Data class representing a state machine's snapshot.
 */
@OptIn(ExperimentalPathApi::class)
data class Snapshot(val lastIncludedLogIndex: Index, val lastIncludedLogTerm: Term, val dataPath: Path) {

    fun save(snapshotPath: Path): Snapshot {
        snapshotPath.createDirectories()
        val destinationMetadataFile = Paths.get(snapshotPath.toString(), METADATA_FILENAME)
        val destinationDataFile = Paths.get(snapshotPath.toString(), DATA_FILENAME)
        val (lastLogIndex, lastLogTerm) = loadMetadata(destinationMetadataFile)
        if (lastLogIndex == lastIncludedLogIndex && lastLogTerm == lastIncludedLogTerm) {
            // The snapshot metadata acts like a signature for a snapshot. No need to save
            // anything since the metadata stored on file system matches the metadata from
            // this snapshot.
            return copy(dataPath = destinationDataFile)
        }
        try {
            saveMetadata(destinationMetadataFile)
            saveData(destinationDataFile)
            return copy(dataPath = destinationDataFile)
        } catch (e: Exception) {
            // Ensure consistency in case of errors while saving the snapshot.
            destinationMetadataFile.runCatching { deleteIfExists() }
            destinationDataFile.runCatching { deleteIfExists() }
            throw e
        }
    }

    private fun saveMetadata(metadataPath: Path) {
        metadataPath.outputStream().use { output ->
            val buffer = ByteBuffer.allocate(Long.SIZE_BYTES * 2)
            buffer.putLong(lastIncludedLogIndex.value)
            buffer.putLong(lastIncludedLogTerm.value)
            buffer.flip()
            output.write(buffer.array())
        }
    }

    private fun saveData(destinationDataPath: Path) {
        dataPath.toFile().copyTo(destinationDataPath.toFile(), overwrite = true)
    }

    companion object {

        private const val METADATA_FILENAME = "metadata.snapshot"
        private const val DATA_FILENAME = "data.snapshot"

        fun load(snapshotPath: Path): Snapshot {
            val metadataFile = Paths.get(snapshotPath.toString(), METADATA_FILENAME)
            val (lastIncludedLogIndex, lastIncludedLogTerm) = loadMetadata(metadataFile)
            val dataFile = Paths.get(snapshotPath.toString(), DATA_FILENAME)
            return Snapshot(lastIncludedLogIndex, lastIncludedLogTerm, dataFile)
        }

        private fun loadMetadata(metadataFile: Path): Pair<Index, Term> {
            return try {
                val buffer = ByteBuffer.wrap(metadataFile.readBytes())
                val lastIncludedLogIndex = buffer.long
                val lastIncludedLogTerm = buffer.long
                Pair(Index(lastIncludedLogIndex), Term(lastIncludedLogTerm))
            } catch (e: IOException) {
                Pair(Index.UNDEFINED, Term.UNDEFINED)
            }
        }
    }
}
