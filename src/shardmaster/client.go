package shardmaster

//
// Shardmaster clerk.
//

import "labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	currentLeader int
	id            int64
	seq	          int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.currentLeader = 0
	ck.id = nrand()
	ck.seq = 1
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.ClientId = ck.id
	args.Seq = ck.seq
	ck.seq++
	i := ck.currentLeader
	for {
		reply := QueryReply{}
		// try each known server.
		ok := ck.servers[i].Call("ShardMaster.Query", args, &reply)
		if ok && reply.WrongLeader == false {
			ck.currentLeader = i
			if reply.Err == OK {
				return reply.Config
			} else if reply.Err == ErrTimeout{
				continue
			}
		}
		time.Sleep(10 * time.Millisecond)
		i = (i + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClientId = ck.id
	args.Seq = ck.seq
	ck.seq++
	i := ck.currentLeader
	for {
		reply := JoinReply{}
		// try each known server.
		ok := ck.servers[i].Call("ShardMaster.Join", args, &reply)
		if ok && reply.WrongLeader == false {
			ck.currentLeader = i
			if reply.Err == OK {
				return
			} else if reply.Err == ErrTimeout{
				continue
			}
		}
		time.Sleep(10 * time.Millisecond)
		i = (i + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClientId = ck.id
	args.Seq = ck.seq
	ck.seq++
	i := ck.currentLeader
	for {
		reply := LeaveReply{}
		// try each known server.
		ok := ck.servers[i].Call("ShardMaster.Leave", args, &reply)
		if ok && reply.WrongLeader == false {
			ck.currentLeader = i
			if reply.Err == OK {
				return
			} else if reply.Err == ErrTimeout{
				continue
			}
		}
		time.Sleep(10 * time.Millisecond)
		i = (i + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClientId = ck.id
	args.Seq = ck.seq
	ck.seq++
	i := ck.currentLeader
	for {
		reply := MoveReply{}
		// try each known server.
		ok := ck.servers[i].Call("ShardMaster.Move", args, &reply)
		if ok && reply.WrongLeader == false {
			ck.currentLeader = i
			if reply.Err == OK {
				return
			} else if reply.Err == ErrTimeout{
				continue
			}
		}
		time.Sleep(10 * time.Millisecond)
		i = (i + 1) % len(ck.servers)
	}
}
