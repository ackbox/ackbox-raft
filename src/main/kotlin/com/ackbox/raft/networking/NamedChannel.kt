package com.ackbox.raft.networking

import io.grpc.CallOptions
import io.grpc.ClientCall
import io.grpc.ManagedChannel
import io.grpc.MethodDescriptor
import java.util.concurrent.TimeUnit

class NamedChannel(val address: NodeAddress, private val channel: ManagedChannel) : ManagedChannel() {

    val id: String = address.nodeId

    override fun authority(): String = channel.authority()

    override fun shutdown(): ManagedChannel = channel.shutdown()

    override fun isShutdown(): Boolean = channel.isShutdown

    override fun isTerminated(): Boolean = channel.isTerminated

    override fun shutdownNow(): ManagedChannel = channel.shutdownNow()

    override fun awaitTermination(timeout: Long, unit: TimeUnit?): Boolean = channel.awaitTermination(timeout, unit)

    override fun <In : Any?, Out : Any?> newCall(d: MethodDescriptor<In, Out>?, o: CallOptions?): ClientCall<In, Out> {
        return channel.newCall(d, o)
    }
}
