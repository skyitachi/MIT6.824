package raftkv

import (
	"bytes"
	"fmt"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
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
	Key    string
	Value  string

	ClientId int64
	Seq      int
}

type Result struct {
	Seq int
	Opname string
	Value  string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvdatabase map[string]string
	detectDup  map[int64]Result
	chanresult map[int]chan Op
}

func (kv *KVServer) CheckSame(c1 Op, c2 Op) bool {
	if c1.ClientId == c2.ClientId && c1.Seq == c2.Seq{
		return true
	}
	return false
}

func (kv *KVServer) StartCommand(oop Op) (Err, string) {
	kv.mu.Lock()
	//if kv.maxraftstate != -1 && kv.rf.GetStateSize() > kv.maxraftstate {
	//	kv.mu.Unlock()
	//	return ErrTimeout, ""
	//}

	if res, ok := kv.detectDup[oop.ClientId]; ok && res.Seq >= oop.Seq {
		resvalue := ""
		if res.Opname == "Get" {
			resvalue = res.Value
		}
		kv.mu.Unlock()
		return OK, resvalue
	}

	index, _, isLeader := kv.rf.Start(oop)
	if !isLeader {
		kv.mu.Unlock()
		return ErrWrongLeader, ""
	}
	fmt.Println("index",index, "log op:", oop.Opname, "key: ", oop.Key, "value: ", oop.Value, "cid: ", oop.ClientId, "seq: ", oop.Seq)
	ch := make(chan Op)
	kv.chanresult[index] = ch
	kv.mu.Unlock()
	defer func() {
		kv.mu.Lock()
		delete(kv.chanresult, index)
		kv.mu.Unlock()
	}()
	//fmt.Println("unlock")
	select {
	case c := <-ch:
		if kv.CheckSame(c, oop) {
			fmt.Println("reply to client:", index)
			val := ""
			if oop.Opname == "Get" {
				kv.mu.Lock()
				val = kv.detectDup[oop.ClientId].Value
				kv.mu.Unlock()
			}
			return OK, val
		} else {
			return ErrWrongLeader, ""
		}
	case <-time.After(time.Millisecond * 2000):
		fmt.Println("timeout index", index)
		return ErrWrongLeader, ""
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	fmt.Println("Get", args.Key, args.ClientId, args.Seq, kv.me)
	op := Op{"Get", args.Key, "", args.ClientId, args.Seq}
	err, val := kv.StartCommand(op)

	reply.Err = err

	if err == ErrWrongLeader {
		reply.WrongLeader = true
		return
	}

	reply.WrongLeader = false
	reply.Value = val
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	fmt.Println(args.Op, args.Key, args.Value,args.ClientId, args.Seq, kv.me)
	op := Op{args.Op, args.Key, args.Value, args.ClientId, args.Seq}
	err, _ := kv.StartCommand(op)
	reply.Err = err
	if err == ErrWrongLeader {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
	}
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

func (kv *KVServer) DupCheck(cliid int64, seqid int) bool {
	res, ok := kv.detectDup[cliid]
	if ok {
		return seqid > res.Seq
	}
	return true
}

func (kv *KVServer) Apply(oop Op) {
	kv.mu.Lock()
	if kv.DupCheck(oop.ClientId, oop.Seq) {
		switch oop.Opname {
		case "Put":
			kv.kvdatabase[oop.Key] = oop.Value
		case "Append":
			if _, ok := kv.kvdatabase[oop.Key]; ok {
				kv.kvdatabase[oop.Key] += oop.Value
			} else {
				kv.kvdatabase[oop.Key] = oop.Value
			}
		}
		kv.detectDup[oop.ClientId] = Result{oop.Seq, oop.Opname, kv.kvdatabase[oop.Key]}
	}
	kv.mu.Unlock()
}

func (kv *KVServer) Reply(oop Op, index int) {
	kv.mu.Lock()
	ch, ok := kv.chanresult[index]
	kv.mu.Unlock()
	if ok {
		select {
		case <-ch:
		default:
		}
		ch <- oop
	}
}

func (kv *KVServer) doApplyOp() {
	for {
		msg := <-kv.applyCh
		if msg.CommandValid {
			index := msg.CommandIndex
			if oop, ok := msg.Command.(Op); ok {
				fmt.Println(kv.me, " will apply committed log: ", index, oop)
				kv.Apply(oop)
				kv.Reply(oop, index)
				if kv.maxraftstate != -1 && kv.rf.GetStateSize() >= kv.maxraftstate{ //&& index == kv.rf.GetCommitIndex() {
					kv.SaveSnapshot(index)
				}
			}
		} else {
			kv.LoadSnapshot(msg.Snapshot)
		}
	}
}

func (kv *KVServer) SaveSnapshot(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvdatabase)
	e.Encode(kv.detectDup)
	data := w.Bytes()
	kv.rf.SaveSnapshot(index, data)
}

func (kv *KVServer) LoadSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		kv.mu.Lock()
		kv.kvdatabase = make(map[string]string)
		kv.mu.Unlock()
		return
	}
	s := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(s)
	var kvdb map[string]string
	var dup map[int64]Result
	if d.Decode(&kvdb) != nil || d.Decode(&dup) != nil {
		fmt.Println("server ", kv.me, " readsnapshot wrong!")
	} else {
		kv.mu.Lock()
		kv.kvdatabase = kvdb
		kv.detectDup = dup
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
	kv.kvdatabase = make(map[string]string)
	kv.detectDup = make(map[int64]Result)
	kv.chanresult = make(map[int]chan Op)

	kv.LoadSnapshot(persister.ReadSnapshot())
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	go kv.doApplyOp()

	return kv
}
