package com.ackbox.raft.support

import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import java.util.UUID

class TemporaryFile(filename: String) {

    private val wrapped: File = Paths.get(createTemporaryDir(), filename).toFile()

    suspend fun <T : Any> use(receiver: suspend (File) -> T): T {
        try {
            return receiver.invoke(wrapped)
        } finally {
            wrapped.runCatching { deleteRecursively() }
        }
    }

    private fun createTemporaryDir(): String {
        return Files.createTempDirectory(UUID.randomUUID().toString()).toAbsolutePath().toString()
    }
}
