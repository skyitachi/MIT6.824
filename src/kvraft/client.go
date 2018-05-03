package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "sync"
//import "fmt"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu            sync.Mutex
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
	// You'll have to add code here.
	ck.currentLeader = 0
	ck.id = nrand()
	ck.seq = 1
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.mu.Lock()
	args := GetArgs{key, ck.id, ck.seq}
	ck.seq++
	ck.mu.Unlock()
	i := ck.currentLeader
	for{
		reply := GetReply{}
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		if ok && reply.WrongLeader == false{
			ck.mu.Lock()
			ck.currentLeader = i
			ck.mu.Unlock()
			if reply.Err == OK{
				return reply.Value
			}else{
				return ""
			}
		}
		i = (i + 1) % len(ck.servers)
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	args := PutAppendArgs{key, value, op, ck.id, ck.seq}
	ck.seq++
	ck.mu.Unlock()
	//fmt.Println(ck.currentLeader)
	i := ck.currentLeader
	for{
		reply := PutAppendReply{}
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.WrongLeader == false{
			ck.mu.Lock()
			ck.currentLeader = i
			ck.mu.Unlock()
			return
		}
		i = (i + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
