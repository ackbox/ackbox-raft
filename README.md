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

```kotlin
// Check the state drift between leader and followers. If state drift is greater
// than maximum allowed, prepare to send a snapshot to the follower.
val stateDrift = log.getLastItemIndex() - state.matchLogIndex
if (stateDrift > config.getMaxAllowedStateDrift()) {
    remoteClient.installSnapshot()
}
```
