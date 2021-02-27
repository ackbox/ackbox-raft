package com.ackbox.raft.support

import com.ackbox.raft.networking.NodeAddress
import com.ackbox.raft.networking.NodeInmemoryAddress
import com.ackbox.raft.networking.NodeNetAddress
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule

val SERIALIZER: ObjectMapper by lazy {
    ObjectMapper()
        .addMixIn(NodeAddress::class.java, NodeAddressMixin::class.java)
        .registerKotlinModule()
}

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes(
    JsonSubTypes.Type(value = NodeNetAddress::class),
    JsonSubTypes.Type(value = NodeInmemoryAddress::class)
)
internal class NodeAddressMixin
