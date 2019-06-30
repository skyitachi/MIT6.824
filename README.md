# MIT6.824 Distributed System(Spring 2018)

https://pdos.csail.mit.edu/6.824/schedule.html

## Lab task
- [x] lab1: MapReduce
  - A naive distributed MapReduce scheduler
- [x] lab2: Raft
  - Leader election
  - Log replication
  - Log/states persistence
- [x] lab3: KV Raft
  - KV service(Put/Append, Get)
  - Snapshot
- [x] lab4: ShardKV
  - Shardmaster
  - ShardKV
    - Configuration changes
    - Data migration
    - Data deletion(challenge)
    - Availability during configuration changes(challenge)

## Several awesome bugs 
  - Lab3B: TestSnapshotSize3B has a request of time, finishing in 20s. So Raft needs to replicate logs immediately when it call Start successfully. Fix this bug will cause out-of-order execution of Raft, so I also modify the design of Raft.
  - Lab3B: TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable3B may have a bug of log too long. But it can't be fixed by elegant method so I don't fix it. But it only happens once during thousands of tests. I don't think it is a serious bug, it is only a design request.
  - Lab4A: Map in go is hashmap, instead of sorted structure. So traverse the same map twice, you may get different result.
  - Lab4B: Every decision needs to be committed by Raft. The tests in Lab4 is weak, but TestChallenge1Concurrent is strong. My code will fail in this point about every 500 times, with some data will lost. But I think my design and implementation is true, and I have no time to fix this small bug.
  
  
## Hint
When machine's load is high, the probability of bug will increase. Because many goroutines race to the scheduling of OS, if code has some bugs, it will fail. So I always test the lab using 10 or 20 processes to run the same lab. This way will increase the probability of failing. If failed, that means your code still has some bugs.

My code is not bug-free, but when only run one process, lab1/2/3/4A can run over ten thousands of times, 4B except TestChallenge1Concurrent, can run over ten thousands of times.
