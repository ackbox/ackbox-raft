package com.ackbox.raft.core

import com.ackbox.raft.support.SERIALIZER
import com.ackbox.raft.types.Index
import com.ackbox.raft.types.Term
import com.fasterxml.jackson.module.kotlin.readValue
import java.io.IOException
import java.nio.file.Path
import java.util.zip.ZipEntry
import java.util.zip.ZipInputStream
import java.util.zip.ZipOutputStream
import kotlin.io.path.ExperimentalPathApi
import kotlin.io.path.createDirectories
import kotlin.io.path.createFile
import kotlin.io.path.div
import kotlin.io.path.inputStream
import kotlin.io.path.outputStream

/**
 * Data class representing the state machine's snapshot.
 */
@OptIn(ExperimentalPathApi::class)
data class Snapshot(val metadata: SnapshotMetadata, val dataPath: Path) {

    val lastIncludedLogIndex: Index = Index(metadata.lastIncludedLogIndex)

    val lastIncludedLogTerm: Term = Term(metadata.lastIncludedLogTerm)

    val compressedFilePath: Path = dataPath / COMPRESSED_SNAPSHOT_FILENAME

    companion object {

        internal const val METADATA_FILENAME: String = "metadata.snapshot"
        internal  const val COMPRESSED_SNAPSHOT_FILENAME: String = "snapshot.zip"

        fun load(sourceDataPath: Path): Snapshot {
            return Snapshot(loadMetadata(sourceDataPath), sourceDataPath)
        }

        fun fromCompressedData(sourceDataPath: Path, destinationDataPath: Path): Snapshot {
            try {
                destinationDataPath.runCatching { toFile().deleteRecursively() }
                destinationDataPath.createDirectories()
                decompress(sourceDataPath, destinationDataPath)
                val metadata = loadMetadata(destinationDataPath)
                return Snapshot(metadata, destinationDataPath)
            } catch (e: Exception) {
                // Ensure consistency in case of errors while saving the snapshot.
                destinationDataPath.runCatching { toFile().deleteRecursively() }
                throw e
            }
        }

        fun fromDecompressedData(
            lastIncludedLogIndex: Index,
            lastIncludedLogTerm: Term,
            sourceDataPath: Path,
            destinationDataPath: Path
        ): Snapshot {
            val metadata = SnapshotMetadata(lastIncludedLogIndex.value, lastIncludedLogTerm.value)
            val loadedMetadata = loadMetadata(destinationDataPath)
            if (metadata.matches(loadedMetadata)) {
                // The snapshot metadata acts like a signature for a snapshot. No need to save anything since the metadata
                // stored on file system matches the metadata from this snapshot.
                return Snapshot(metadata, destinationDataPath)
            }
            try {
                destinationDataPath.toFile().deleteRecursively()
                destinationDataPath.createDirectories()
                saveMetadata(metadata, destinationDataPath)
                saveData(sourceDataPath, destinationDataPath)
                compress(destinationDataPath)
                return Snapshot(metadata, destinationDataPath)
            } catch (e: Exception) {
                // Ensure consistency in case of errors while saving the snapshot.
                destinationDataPath.runCatching { toFile().deleteRecursively() }
                throw e
            }
        }

        private fun compress(destinationDataPath: Path) {
            val compressedPath = destinationDataPath / COMPRESSED_SNAPSHOT_FILENAME
            ZipOutputStream(compressedPath.createFile().outputStream()).use { output ->
                destinationDataPath.toFile().walkTopDown().filter { it.isFile }.forEach { file ->
                    output.putNextEntry(ZipEntry(file.name))
                    file.inputStream().copyTo(output)
                }
            }
        }

        private fun decompress(sourceDataPath: Path, destinationDataPath: Path) {
            val compressedPath = sourceDataPath / COMPRESSED_SNAPSHOT_FILENAME
            ZipInputStream(compressedPath.inputStream()).use { input ->
                var entry = input.nextEntry
                while (entry != null) {
                    val destinationFilePath = destinationDataPath / entry.name
                    destinationFilePath.outputStream().use { output -> input.copyTo(output) }
                    entry = input.nextEntry
                }
            }
        }

        private fun saveMetadata(metadata: SnapshotMetadata, destinationDataPath: Path) {
            val metadataPath = destinationDataPath / METADATA_FILENAME
            SERIALIZER.writeValue(metadataPath.createFile().toFile(), metadata)
        }

        private fun loadMetadata(sourceDataPath: Path): SnapshotMetadata {
            return try {
                val metadataPath = sourceDataPath / METADATA_FILENAME
                SERIALIZER.readValue(metadataPath.toFile())
            } catch (e: IOException) {
                SnapshotMetadata(Index.UNDEFINED.value, Term.UNDEFINED.value)
            }
        }

        private fun saveData(sourceDataPath: Path, destinationDataPath: Path) {
            sourceDataPath.toFile().copyRecursively(destinationDataPath.toFile(), overwrite = true)
        }
    }

    data class SnapshotMetadata(val lastIncludedLogIndex: Long, val lastIncludedLogTerm: Long) {

        fun matches(other: SnapshotMetadata): Boolean {
            return lastIncludedLogIndex == other.lastIncludedLogIndex && lastIncludedLogTerm == other.lastIncludedLogTerm
        }
    }
}
