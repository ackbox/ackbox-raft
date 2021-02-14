package com.ackbox.raft.support

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import kotlin.reflect.KClass

class NodeLogger(private val nodeId: String, private val logger: Logger) : Logger by logger {

    override fun debug(msg: String?) = logger.debug("${prefix()} $msg")
    override fun debug(msg: String?, t: Throwable?) = logger.debug("${prefix()} $msg", t)
    override fun debug(format: String?, arg: Any?) = logger.debug("${prefix()} $format", arg)
    override fun debug(format: String?, arg1: Any?, arg2: Any?) = logger.debug("${prefix()} $format", arg1, arg2)
    override fun debug(format: String?, vararg arguments: Any?) = logger.debug("${prefix()} $format", *arguments)

    override fun info(msg: String?) = logger.info("${prefix()} $msg")
    override fun info(msg: String?, t: Throwable?) = logger.info("${prefix()} $msg", t)
    override fun info(format: String?, arg: Any?) = logger.info("${prefix()} $format", arg)
    override fun info(format: String?, arg1: Any?, arg2: Any?) = logger.info("${prefix()} $format", arg1, arg2)
    override fun info(format: String?, vararg arguments: Any?) = logger.info("${prefix()} $format", *arguments)

    override fun warn(msg: String?) = logger.warn("${prefix()} $msg")
    override fun warn(msg: String?, t: Throwable?) = logger.warn("${prefix()} $msg", t)
    override fun warn(format: String?, arg: Any?) = logger.warn("${prefix()} $format", arg)
    override fun warn(format: String?, arg1: Any?, arg2: Any?) = logger.warn("${prefix()} $format", arg1, arg2)
    override fun warn(format: String?, vararg arguments: Any?) = logger.warn("${prefix()} $format", *arguments)

    override fun error(msg: String?) = logger.error("${prefix()} $msg")
    override fun error(msg: String?, t: Throwable?) = logger.error("${prefix()} $msg", t)
    override fun error(format: String?, arg: Any?) = logger.error("${prefix()} $format", arg)
    override fun error(format: String?, arg1: Any?, arg2: Any?) = logger.error("${prefix()} $format", arg1, arg2)
    override fun error(format: String?, vararg arguments: Any?) = logger.error("${prefix()} $format", *arguments)

    private fun prefix(): String = "[node=$nodeId]"

    companion object {

        fun from(nodeId: String, clazz: KClass<*>): NodeLogger {
            val delegate = LoggerFactory.getLogger(clazz.java)
            return NodeLogger(nodeId, delegate)
        }
    }
}
