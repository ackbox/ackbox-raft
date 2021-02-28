package com.ackbox.raft.support

import com.ackbox.raft.types.Partition
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import kotlin.reflect.KClass

class NodeLogger(private val id: String, private val logger: Logger) : Logger by logger {

    override fun debug(msg: String?) = logger.debug("$id $msg")
    override fun debug(msg: String?, t: Throwable?) = logger.debug("$id $msg", t)
    override fun debug(format: String?, arg: Any?) = logger.debug("$id $format", arg)
    override fun debug(format: String?, arg1: Any?, arg2: Any?) = logger.debug("$id $format", arg1, arg2)
    override fun debug(format: String?, vararg arguments: Any?) = logger.debug("$id $format", *arguments)

    override fun info(msg: String?) = logger.info("$id $msg")
    override fun info(msg: String?, t: Throwable?) = logger.info("$id $msg", t)
    override fun info(format: String?, arg: Any?) = logger.info("$id $format", arg)
    override fun info(format: String?, arg1: Any?, arg2: Any?) = logger.info("$id $format", arg1, arg2)
    override fun info(format: String?, vararg arguments: Any?) = logger.info("$id $format", *arguments)

    override fun warn(msg: String?) = logger.warn("$id $msg")
    override fun warn(msg: String?, t: Throwable?) = logger.warn("$id $msg", t)
    override fun warn(format: String?, arg: Any?) = logger.warn("$id $format", arg)
    override fun warn(format: String?, arg1: Any?, arg2: Any?) = logger.warn("$id $format", arg1, arg2)
    override fun warn(format: String?, vararg arguments: Any?) = logger.warn("$id $format", *arguments)

    override fun error(msg: String?) = logger.error("$id $msg")
    override fun error(msg: String?, t: Throwable?) = logger.error("$id $msg", t)
    override fun error(format: String?, arg: Any?) = logger.error("$id $format", arg)
    override fun error(format: String?, arg1: Any?, arg2: Any?) = logger.error("$id $format", arg1, arg2)
    override fun error(format: String?, vararg arguments: Any?) = logger.error("$id $format", *arguments)

    companion object {

        fun forNode(nodeId: String, clazz: KClass<*>): NodeLogger {
            val delegate = LoggerFactory.getLogger(clazz.java)
            val id = "[node=$nodeId]"
            return NodeLogger(id, delegate)
        }

        fun forPartition(nodeId: String, partition: Partition, clazz: KClass<*>): NodeLogger {
            val delegate = LoggerFactory.getLogger(clazz.java)
            val id = "[node=$nodeId] [partition=${partition.value}]"
            return NodeLogger(id, delegate)
        }
    }
}
