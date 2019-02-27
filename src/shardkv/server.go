package shardkv


// import "shardmaster"
import (
	"bytes"
	"fmt"
	"labrpc"
	"time"
)
import "raft"
import "sync"
import "labgob"
import "shardmaster"



type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Opname string

	Key string
	Value string

	NewConfig shardmaster.Config
	MapKV    map[string]string

	ClientId int64
	Seq int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	servers      []*labrpc.ClientEnd

	// Your definitions here.
	kvdatabase 	 map[string]string
	lastconfig   shardmaster.Config
	mck			 *shardmaster.Clerk
	DetectDup 	 map[int64]int
	DetectConfigDup 	 map[int64]int
	result 		 map[int]chan Op

	seq          int
	cid      	 int64
}

func (kv *ShardKV) CheckKey(key string) bool{
	shard := key2shard(key)
	if kv.lastconfig.Shards[shard] != kv.gid {
		return false
	}
	return true
}

func (kv *ShardKV) DupCheck(id int64, seq int) bool {
	ssq, ok := kv.DetectDup[id]
	if ok{
		return seq > ssq
	}
	return true
}

func (kv *ShardKV) EqualConfig(c1 shardmaster.Config, c2 shardmaster.Config) bool{
	ans := true
	if c1.Num != c2.Num {
		ans = false
	}
	return ans
}

func (kv *ShardKV) EqualOp(op1 Op, op2 Op) bool {
	ans := true
	if op1.Opname != op2.Opname || op1.Key != op2.Key || op1.Value != op2.Value || op1.ClientId != op2.ClientId || op1.Seq != op2.Seq {
		ans = false
	}
	if !kv.EqualConfig(op1.NewConfig, op2.NewConfig) {
		ans = false
	}
	for k, v := range op1.MapKV {
		vv, ok := op2.MapKV[k]
		if !ok || vv != v{
			ans = false
		}
	}
	for k, v := range op2.MapKV {
		vv, ok := op1.MapKV[k]
		if !ok || vv != v{
			ans = false
		}
	}
	return ans
}

func (kv *ShardKV) StartCommand(op Op) Err{
	kv.mu.Lock()
	if !kv.CheckKey(op.Key) {
		kv.mu.Unlock()
		return ErrWrongGroup
	}

	if !kv.DupCheck(op.ClientId, op.Seq){
		kv.mu.Unlock()
		return OK
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.mu.Unlock()
		return ErrWrongLeader
	}
	//fmt.Println("index",index, sm.me, op)
	ch := make(chan Op)
	kv.result[index] = ch
	kv.mu.Unlock()
	defer func(){
		kv.mu.Lock()
		delete(kv.result, index)
		kv.mu.Unlock()
	}()

	select{
	case c := <-ch:
		if kv.EqualOp(c, op) {
			return OK
		} else {
			return ErrWrongGroup
		}
	case <- time.After(time.Millisecond * 2000):
		fmt.Println("timeout")
		return ErrTimeout
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{"Get", args.Key, "", shardmaster.Config{}, make(map[string]string), args.Id, args.Seq}
	err := kv.StartCommand(op)

	reply.Err = err
	reply.WrongLeader = false
	fmt.Println("Get gid: ", kv.gid, err, args.Key)
	if err == ErrWrongGroup{
		return
	}
	if err == ErrWrongLeader{
		reply.WrongLeader = true
		return
	}
	if err == ErrTimeout{
		return
	}
	kv.mu.Lock()
	value, ok := kv.kvdatabase[args.Key]
	if !ok {
		value = ""
		reply.Err = ErrNoKey
	}
	reply.Value = value
	kv.mu.Unlock()
	fmt.Println("Get gid: ", kv.gid, err, "Key: ", args.Key, "shard: ", key2shard(args.Key), "value: ", value)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//fmt.Println(args.Op, args.Key, args.Value)
	//fmt.Println(args.Op, kv.gid, "key: ", args.Key, "shard: ", key2shard(args.Key), "value:", args.Value)

	op := Op{args.Op, args.Key, args.Value, shardmaster.Config{}, make(map[string]string), args.Id, args.Seq}
	err := kv.StartCommand(op)
	reply.Err = err
	reply.WrongLeader = false
	fmt.Println(args.Op, kv.gid, err, "key: ", args.Key, "shard: ", key2shard(args.Key), "value:", kv.kvdatabase[args.Key])

	if err == ErrWrongGroup{
		return
	}
	if err == ErrWrongLeader {
		reply.WrongLeader = true
		return
	}
	if err == ErrTimeout{
		return
	}
	//fmt.Println(args.Op, kv.gid, err, "key: ", args.Key, "shard: ", key2shard(args.Key), "value:", args.Value)
}


//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) StartReConfig(op Op) Err{
	kv.mu.Lock()

	if !kv.DupCheck(op.ClientId, op.Seq){
		kv.mu.Unlock()
		return OK
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.mu.Unlock()
		return ErrWrongLeader
	}
	//fmt.Println("index",index, kv.me, op)
	ch := make(chan Op)
	kv.result[index] = ch
	kv.mu.Unlock()
	defer func(){
		kv.mu.Lock()
		delete(kv.result, index)
		kv.mu.Unlock()
	}()

	select{
	case c := <-ch:
		if kv.EqualOp(c, op) {
			return OK
		} else {
			return ErrWrongLeader
		}
	case <- time.After(time.Millisecond * 2000):
		fmt.Println("timeout")
		return ErrTimeout
	}
}

func (kv *ShardKV) Copy(c1 shardmaster.Config, c2 shardmaster.Config) {
	c1.Num = c2.Num
	for i := 0; i < shardmaster.NShards; i++ {
		c1.Shards[i] = c2.Shards[i]
	}
	c1.Groups = make(map[int][]string)
	for k, v := range c2.Groups {
		c1.Groups[k] = make([]string, len(v), cap(v))
		for i := 0; i < len(v); i++ {
			c1.Groups[k] = append(c1.Groups[k], v[i])
		}
	}
}

func (kv *ShardKV) GetConfig() {
	for {
		newconfig := kv.mck.Query(-1)
		//kv.mu.Lock()
		//flag := kv.EqualConfig(newconfig, kv.lastconfig)
		////reject to access the migrate data
		////kv.lastconfig.Num = newconfig.Num
		////kv.lastconfig.Groups = newconfig.Groups
		////for i := 0; i < shardmaster.NShards; i++ {
		////	if kv.lastconfig.Shards[i] != kv.gid && kv.lastconfig.Shards[i] !=0 && newconfig.Shards[i] == kv.gid {
		////		continue
		////	} else {
		////		kv.lastconfig.Shards[i] = newconfig.Shards[i]
		////	}
		////}
		////fmt.Println(kv.gid, kv.me, "get new config", kv.lastconfig)
		//kv.mu.Unlock()
		_, isLeader := kv.rf.GetState()

		if isLeader {
			kv.mu.Lock()
			flag := kv.EqualConfig(newconfig, kv.lastconfig)
			if !flag {
				kv.seq++
				//fmt.Println(kv.me, kv.gid, "get new config")
				op := Op{"ReConfig", "", "", newconfig, make(map[string]string), kv.cid, kv.seq}
				kv.mu.Unlock()
				for{
					err := kv.StartReConfig(op)
					fmt.Println(kv.gid, kv.me, err, kv.lastconfig)
					if err == OK || err == ErrWrongLeader{
						break
					}
					time.Sleep(100 * time.Millisecond)
				}
			} else {
				kv.mu.Unlock()
			}
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) MigrateShard(args *MigrateArgs, reply *MigrateReply){
	op := Op{"Migrate", "", "", shardmaster.Config{}, args.MapKV, args.Id, args.Seq}
	err := kv.StartReConfig(op)

	reply.Err = err
	reply.WrongLeader = false
	if err == ErrWrongLeader{
		reply.WrongLeader = true
	}
}

//func (kv *ShardKV) ApplyNewConfig(cfg shardmaster.Config){
//
//
//}

func (kv *ShardKV) Apply(oop Op) bool {
	kv.mu.Lock()
	if kv.DupCheck(oop.ClientId, oop.Seq) {
		switch oop.Opname {
		case "Put":
			if !kv.CheckKey(oop.Key) {
				kv.mu.Unlock()
				return false
			}
			kv.kvdatabase[oop.Key] = oop.Value
			kv.DetectDup[oop.ClientId] = oop.Seq
		case "Append":
			if !kv.CheckKey(oop.Key) {
				kv.mu.Unlock()
				return false
			}
			if _, ok := kv.kvdatabase[oop.Key]; ok {
				kv.kvdatabase[oop.Key] += oop.Value
			} else {
				kv.kvdatabase[oop.Key] = oop.Value
			}
			kv.DetectDup[oop.ClientId] = oop.Seq
		case "ReConfig":
			//kv.ApplyNewConfig(oop.NewConfig)
			cfg := oop.NewConfig
			kv.lastconfig.Num = cfg.Num
			kv.lastconfig.Groups = cfg.Groups
			mapkv := make(map[int]map[string]string)
			for k, v := range kv.kvdatabase {
				shard := key2shard(k)
				if cfg.Shards[shard] != kv.gid {
					if mapkv[cfg.Shards[shard]] == nil {
						mapkv[cfg.Shards[shard]] = make(map[string]string)
					}
					mapkv[cfg.Shards[shard]][k] = v
					//kv.lastconfig.Shards[shard] = cfg.Shards[shard]
					delete(kv.kvdatabase, k)
				}
			}
			for i := 0; i < shardmaster.NShards; i++ {
				if kv.lastconfig.Shards[i] != kv.gid && kv.lastconfig.Shards[i] !=0 && cfg.Shards[i] == kv.gid {
					continue
				} else {
					kv.lastconfig.Shards[i] = cfg.Shards[i]
				}
			}
			kv.mu.Unlock()

			//migrate rpc
			_, isLeader := kv.rf.GetState()
			if isLeader {
				for g, v := range mapkv {
					group := cfg.Groups[g]
					si := 0
					kv.mu.Lock()
					args := MigrateArgs{v, kv.cid, kv.seq}
					kv.seq++
					kv.mu.Unlock()

					for {
						var reply MigrateReply
						srv := kv.make_end(group[si])
						ok := srv.Call("ShardKV.MigrateShard", &args, &reply)
						if ok && reply.WrongLeader == false {
							if reply.Err == OK {
								break
							} else if reply.Err == ErrTimeout{
								continue
							}
						}

						si = (si + 1) % len(group)
					}
				}
			}
			fmt.Println(kv.gid, kv.me, "start migrate ", kv.lastconfig)
			kv.mu.Lock()
		case "Migrate":
			for k, v := range oop.MapKV {
				kv.kvdatabase[k] = v
				shard := key2shard(k)
				kv.lastconfig.Shards[shard] = kv.gid
			}
			fmt.Println(kv.gid, kv.me, "migrate finish", kv.lastconfig)
		}
	}
	kv.mu.Unlock()
	return true
}

func (kv *ShardKV) Reply(oop Op, index int) {
	kv.mu.Lock()
	ch, ok := kv.result[index]
	kv.mu.Unlock()
	if ok {
		select {
		case <-ch:
		default:
		}
		ch <- oop
	}
}

func (kv *ShardKV) doApplyOp() {
	for {
		msg := <-kv.applyCh
		if msg.CommandValid {
			index := msg.CommandIndex
			if oop, ok := msg.Command.(Op); ok {
				flag := kv.Apply(oop)
				if !flag {
					kv.Reply(Op{NewConfig: shardmaster.Config{Num: -2}}, index)
				} else {
					kv.Reply(oop, index)
				}
				if kv.maxraftstate != -1 && 10*kv.rf.GetStateSize() >= kv.maxraftstate {
					kv.SaveSnapshot(index)
				}
			}
		} else {
			kv.LoadSnapshot(msg.Snapshot)
		}
	}
}

func (kv *ShardKV) SaveSnapshot(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvdatabase)
	e.Encode(kv.DetectDup)
	data := w.Bytes()
	kv.rf.SaveSnapshot(index, data)
}

func (kv *ShardKV) LoadSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		kv.mu.Lock()
		kv.kvdatabase = make(map[string]string)
		kv.mu.Unlock()
		return
	}
	s := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(s)
	var kvdb map[string]string
	var dup map[int64]int
	if d.Decode(&kvdb) != nil || d.Decode(&dup) != nil {
		//fmt.Println("server ", kv.me, " readsnapshot wrong!")
	} else {
		kv.mu.Lock()
		kv.kvdatabase = kvdb
		kv.DetectDup = dup
		kv.mu.Unlock()
	}
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters
	kv.servers = servers
	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.kvdatabase = make(map[string]string)
	kv.DetectDup = make(map[int64]int)
	kv.result = make(map[int]chan Op)

	kv.lastconfig = kv.mck.Query(-1)

	kv.seq = 0
	kv.cid = nrand()

	kv.LoadSnapshot(persister.ReadSnapshot())
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.GetConfig()
	go kv.doApplyOp()
	return kv
}
