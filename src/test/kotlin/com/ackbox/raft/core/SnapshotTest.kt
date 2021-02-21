package com.ackbox.raft.core

import com.ackbox.raft.support.SERIALIZER
import com.ackbox.raft.types.Index
import com.ackbox.raft.types.Term
import com.ackbox.random.krandom
import com.fasterxml.jackson.module.kotlin.readValue
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.nio.file.Path
import java.nio.file.Paths
import java.util.UUID
import kotlin.io.path.ExperimentalPathApi
import kotlin.io.path.createDirectories
import kotlin.io.path.div

@OptIn(ExperimentalPathApi::class)
internal class SnapshotTest {

    @TempDir // Needs to be non-private.
    internal lateinit var baseFolder: File

    @Test
    fun `should compress snapshot when created from decompressed data`() {
        val sourceDataPath = Paths.get(baseFolder.absolutePath, "source").createDirectories()
        val destinationDataPath = Paths.get(baseFolder.absolutePath, "destination").createDirectories()
        val object1 = saveSnapshotObject(sourceDataPath)
        val object2 = saveSnapshotObject(sourceDataPath)

        val expectedFiles = listOf(
            object1.filename,
            object2.filename,
            Snapshot.METADATA_FILENAME,
            Snapshot.COMPRESSED_SNAPSHOT_FILENAME
        )

        Snapshot.fromDecompressedData(INDEX, TERM, sourceDataPath, destinationDataPath)

        val actualFiles = destinationDataPath.toFile().walkTopDown().filter { it.isFile }.map { it.name }.toList()
        assertEquals(expectedFiles.sorted(), actualFiles.sorted())
    }

    @Test
    fun `should load snapshot from compressed data`() {
        val sourceDataPath = Paths.get(baseFolder.absolutePath, "source").createDirectories()
        val destination1DataPath = Paths.get(baseFolder.absolutePath, "destination1").createDirectories()
        val destination2DataPath = Paths.get(baseFolder.absolutePath, "destination2").createDirectories()

        val savedObject1 = saveSnapshotObject(sourceDataPath)
        val savedObject2 = saveSnapshotObject(sourceDataPath)

        val fromDecompressed = Snapshot.fromDecompressedData(INDEX, TERM, sourceDataPath, destination1DataPath)
        val fromCompressed = Snapshot.fromCompressedData(destination1DataPath, destination2DataPath)

        val loadedObject1 = loadSnapshotObject(destination2DataPath, savedObject1.filename)
        val loadedObject2 = loadSnapshotObject(destination2DataPath, savedObject2.filename)

        assertEquals(fromDecompressed.lastIncludedLogIndex, fromCompressed.lastIncludedLogIndex)
        assertEquals(fromDecompressed.lastIncludedLogTerm, fromCompressed.lastIncludedLogTerm)
        assertEquals(fromDecompressed.dataPath, destination1DataPath)
        assertEquals(fromCompressed.dataPath, destination2DataPath)
        assertEquals(savedObject1, loadedObject1)
        assertEquals(savedObject2, loadedObject2)
    }

    private fun loadSnapshotObject(path: Path, filename: String): SnapshotObject {
        val sourcePath = path / filename
        return SERIALIZER.readValue(sourcePath.toFile())
    }

    private fun saveSnapshotObject(path: Path): SnapshotObject {
        val snapshotObject = SnapshotObject()
        val destinationPath = path / snapshotObject.filename
        SERIALIZER.writeValue(destinationPath.toFile(), snapshotObject)
        return snapshotObject
    }

    private data class SnapshotObject(val filename: String = UUID.randomUUID().toString())

    companion object {

        private val INDEX: Index = krandom()
        private val TERM: Term = krandom()
    }
}
