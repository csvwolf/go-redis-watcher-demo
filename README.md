# Redis Watcher Demo
关于如何监听 Redis 中的到期键，这里实现了一个有补偿的 demo。

核心实现原理：<https://redis.io/docs/latest/develop/use/keyspace-notifications/>

但是因为 Redis 键空间的消息并不会持久化，因此发版会导致丢事件，所以设计用 Redis 自己补偿自己。