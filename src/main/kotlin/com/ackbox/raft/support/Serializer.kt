package com.ackbox.raft.support

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule

val SERIALIZER: ObjectMapper = ObjectMapper().registerKotlinModule()

