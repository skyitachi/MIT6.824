package raftkv

import (
	"labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

//import "fmt"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	currentLeader int
	id            int64
	seq           int

	//rpcfail		  int
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

	//ck.rpcfail = 0
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
	args := GetArgs{key, ck.id, ck.seq}
	ck.seq++
	i := ck.currentLeader
	for {
		reply := GetReply{}
		//fmt.Println("send rpc to ", i)
		//ck.rpcfail++
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		if ok && reply.WrongLeader == false {
			ck.currentLeader = i
			//fmt.Println("leader is ", i)
			if reply.Err == OK {
				return reply.Value
			} else if reply.Err == ErrNoKey{
				return ""
			} else if reply.Err == ErrTimeout{
				continue
			}
		}
		//if !ok {
		//	ck.rpcfail++
		//	fmt.Println(ck.rpcfail)
		//}
		//if reply.WrongLeader {
		//	fmt.Println("wrong leader")
		//}
		time.Sleep(time.Duration(10) * time.Millisecond)
		i = (i + 1) % len(ck.servers)
	}
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
	args := PutAppendArgs{key, value, op, ck.id, ck.seq}
	ck.seq++
	//fmt.Println(ck.currentLeader)
	i := ck.currentLeader
	for {
		reply := PutAppendReply{}
		//fmt.Println("send rpc to ", i)
		//ck.rpcfail++
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.WrongLeader == false {
			ck.currentLeader = i
			//fmt.Println("leader is ", i)
			if reply.Err == OK {
				return
			} else if reply.Err == ErrTimeout {
				continue
			}
		}
		//if !ok {
		//	ck.rpcfail++
		//	fmt.Println(ck.rpcfail)
		//}
		//if reply.WrongLeader {
		//	fmt.Println("wrong leader")
		//}
		time.Sleep(time.Duration(10) * time.Millisecond)
		i = (i + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
	//fmt.Println("ck rpc number: ", ck.rpcfail)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
	//fmt.Println("ck rpc number: ", ck.rpcfail)
}
