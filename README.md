## Ackbox Raft

Simple implementation of Raft consensus algorithm.

Reference: https://raft.github.io/raft.pdf

### TODO:

- Snapshot save and restore.
- Implementation for replicated log.
- Dynamic configuration.
- Improve response codes.
- Make set and get return multiple entries.
- Make set and get accept custom key.
- Trim log size.

```kotlin
// Check the state drift between leader and followers. If state drift is greater
// than maximum allowed, prepare to send a snapshot to the follower.
val stateDrift = log.getLastItemIndex() - state.matchLogIndex
if (stateDrift > config.getMaxAllowedStateDrift()) {
    remoteClient.installSnapshot()
}
```

```kotlin
@Test
fun `should reject get of out-of-bounds log items`() {
    val config = createConfig(UUID.randomUUID().toString())
    val segmentedLog = SegmentedLog(config)
    val firstItemIndex = segmentedLog.getFirstItemIndex()
    val firstItem = Fixtures.createLogItem(firstItemIndex)
    val outOfLowerBoundItemIndex = firstItemIndex - INDEX_DIFF
    val outOfUpperBoundItemIndex = firstItemIndex + INDEX_DIFF

    segmentedLog.use { log ->
        log.appendItems(listOf(firstItem))
        assertEquals(firstItem, log.getItem(firstItemIndex))
        assertThrows<IllegalStateException> { log.getItem(outOfLowerBoundItemIndex) }
        assertThrows<IllegalStateException> { log.getItem(outOfUpperBoundItemIndex) }
    }
}
```
