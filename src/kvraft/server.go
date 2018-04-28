package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
	"fmt"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
		fmt.Println("")
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Opname string
	Key string
	Value string

	ClientId int64
	Seq int
}

const(
    IDLE=iota
    BUSY
)

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvdatabase map[string]string
	detectDup  map[int64]int
	chanresult map[int]chan Op
}


func (kv *KVServer) StartCommand(oop Op) Err {
	index, _, isLeader := kv.rf.Start(oop)
	fmt.Println("index",index)
	if !isLeader{
		return ErrWrongLeader
	}
	kv.mu.Lock()
	ch, ok := kv.chanresult[index]
	if !ok{
		ch = make(chan Op)
		kv.chanresult[index] = ch
	}
	kv.mu.Unlock()
	//fmt.Println("unlock")
	select{
	case c := <-ch:
		if c == oop{
			fmt.Println("reply to client:",index)
			return OK
		}else{
			return ErrWrongLeader
		}
	case <-time.After(time.Second * 10):
		fmt.Println("timeout index", index)
		return ErrTimeout
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//fmt.Println("Get", args.ClientId, args.Seq, kv.me)
	op := Op{"Get", args.Key, "", args.ClientId, args.Seq}
	err := kv.StartCommand(op)

	if err == ErrWrongLeader || err == ErrTimeout{
		reply.WrongLeader = true
	}else{
		reply.WrongLeader = false
	}
	reply.Err = err
	kv.mu.Lock()
	value, ok := kv.kvdatabase[args.Key]
	if !ok{
		value = ""
		reply.Err = ErrNoKey
	}
	reply.Value = value
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//fmt.Println(args.Op, args.ClientId, args.Seq,kv.me)
	op := Op{args.Op, args.Key, args.Value, args.ClientId, args.Seq}
	err := kv.StartCommand(op)
	if err == ErrWrongLeader || err == ErrTimeout{
		reply.WrongLeader = true
	}else{
		reply.WrongLeader = false
	}
	reply.Err = err
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

// func (kv *KVServer) DupCheck(cliid int64, seqid int) bool {
// 	id, ok := kv.detectDup[cliid]
// 	if ok{
// 		return seqid > id
// 	}
// 	return true
// }

func (kv *KVServer) Apply(oop Op){
	if kv.detectDup[oop.ClientId] < oop.Seq{
		switch oop.Opname{
		case "Put":
			kv.kvdatabase[oop.Key] = oop.Value
		case "Append":
			kv.kvdatabase[oop.Key] += oop.Value
		}
		kv.detectDup[oop.ClientId] = oop.Seq
	}
}

func (kv *KVServer) Reply(oop Op, index int){
		ch, ok := kv.chanresult[index]
		if ok{
			select{
				case <- ch:
				default:
			}
			ch <- oop
		}else{
			ch = make(chan Op)
			kv.chanresult[index] = ch
		}
}

func (kv *KVServer) doApplyOp(){
	for{
		msg := <-kv.applyCh
		index := msg.CommandIndex
		kv.mu.Lock()
		oop := msg.Command.(Op)
		kv.Apply(oop)
		kv.Reply(oop, index)
		kv.mu.Unlock()
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvdatabase = make(map[string]string)
	kv.detectDup = make(map[int64]int)
	kv.chanresult = make(map[int]chan Op)

	go kv.doApplyOp()

	return kv
}
