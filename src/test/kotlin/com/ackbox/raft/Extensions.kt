package com.ackbox.raft

import com.ackbox.raft.log.Segment
import com.ackbox.raft.log.SegmentedLog

fun <T : Any> Segment.use(function: (Segment) -> T) {
    open()
    try {
        function(this)
    } finally {
        close()
    }
}

fun <T : Any> SegmentedLog.use(function: (SegmentedLog) -> T) {
    open()
    try {
        function(this)
    } finally {
        close()
    }
}
