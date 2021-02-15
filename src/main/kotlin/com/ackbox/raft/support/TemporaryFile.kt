package com.ackbox.raft.support

import java.io.File

class TemporaryFile(prefix: String) {

    private val wrapped: File = createTemporaryFile(prefix)

    suspend fun <T : Any> use(receiver: suspend (File) -> T): T {
        try {
            return receiver.invoke(wrapped)
        } finally {
            wrapped.runCatching { delete() }
        }
    }

    private fun createTemporaryFile(prefix: String): File {
        return File.createTempFile(prefix, ".tmp")
    }
}
