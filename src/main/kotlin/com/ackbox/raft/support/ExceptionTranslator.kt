package com.ackbox.raft.support

import io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener
import io.grpc.Metadata
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
import io.grpc.ServerInterceptor
import io.grpc.Status

class ExceptionTranslator : ServerInterceptor {

    override fun <ReqT : Any?, RespT : Any?> interceptCall(
        call: ServerCall<ReqT, RespT>,
        metadata: Metadata,
        next: ServerCallHandler<ReqT, RespT>
    ): ServerCall.Listener<ReqT> {
        val listener: ServerCall.Listener<ReqT> = next.startCall(call, metadata)
        return ServerCallListener(listener, call, metadata)
    }

    private class ServerCallListener<ReqT, RespT>(
        listener: ServerCall.Listener<ReqT>,
        private val call: ServerCall<ReqT, RespT>,
        private val metadata: Metadata
    ) : SimpleForwardingServerCallListener<ReqT>(listener) {

        override fun onHalfClose() {
            try {
                super.onHalfClose()
            } catch (ex: Exception) {
                handleException(ex, call, metadata)
                throw ex
            }
        }

        override fun onReady() {
            try {
                super.onReady()
            } catch (ex: Exception) {
                handleException(ex, call, metadata)
                throw ex
            }
        }

        private fun handleException(e: Exception, call: ServerCall<ReqT, RespT>, metadata: Metadata) {
            when (e) {
                is RetryableException -> call.close(Status.UNAVAILABLE.withDescription(e.message), metadata)
                is NonRetryableException -> call.close(Status.INTERNAL.withDescription(e.message), metadata)
                else -> call.close(Status.INTERNAL, metadata)
            }
        }
    }
}
