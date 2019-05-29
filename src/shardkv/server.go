package shardkv


// import "shardmaster"
import (
	"fmt"
	"labrpc"
	"log"
	"shardmaster"
	"time"
)
import "raft"
import "sync"
import "labgob"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
		fmt.Println("")
	}
	return
}


//get and putappend
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Opname string
	Key    string
	Value  string
	Shard  []int //pull or del

	ClientId int64
	Seq      int

	err    Err
}

//re-configuration
type Cfg struct {
	NewConfig shardmaster.Config

	ClientId int64
	Seq int
}

//migrate
type Migrate struct {
	MapKV  map[string]string

	Num   int
	Gid   int
}

//delete
type Delop struct {
	// TODO
}

const (
	Alive = iota
	Killed
)

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	state        int

	mck	         *shardmaster.Clerk
	detectDup	 map[int64]int
	kvdatabase   map[string]string
	chanresult   map[int]chan Op


	myconfig     [2]shardmaster.Config //0 is now, 1 is new // 0 + need is current state
	//when recv from other group and apply to my group, the shard is in my group
	//when others query from me, the shard is not in my group, cannot serve
	myshards	 [shardmaster.NShards]int //shard -> 0/1
	needrecv     map[int][]int 			  // gid->[]shard
	needsend     [shardmaster.NShards]int //shard->gid/-1

	migrateDup   map[int]int

}

func (kv *ShardKV) CheckSame(c1 Op, c2 Op) bool {
	if c1.ClientId == c2.ClientId && c1.Seq == c2.Seq{
		return true
	}
	return false
}

// only judge from lastconfig
// maybe need to record more
func (kv *ShardKV) CheckGroup(oop Op) bool {
	shard := key2shard(oop.Key)
	in := kv.myshards[shard]
	if in == 1 {
		return true
	}
	return false
}

func (kv *ShardKV) MakeEmptyConfig() shardmaster.Config{
	res := shardmaster.Config{}
	res.Num = 0;
	for i := 0; i < shardmaster.NShards; i++ {
		res.Shards[i] = 0
	}
	res.Groups = make(map[int][]string)
	return res
}

func (kv *ShardKV) CopyConfig(c1 *shardmaster.Config, c2 *shardmaster.Config) {
	if c2 == nil {
		return
	}
	c1.Num = c2.Num
	for i := 0; i < shardmaster.NShards; i++ {
		c1.Shards[i] = c2.Shards[i]
	}
	c1.Groups = make(map[int][]string)
	if c2.Groups == nil {
		return
	}
	for k, v := range c2.Groups {
		c1.Groups[k] = make([]string, len(v))
		for i := 0; i < len(v); i++ {
			c1.Groups[k][i] = v[i]
		}
	}
}


//func (kv *ShardKV) CheckSend(oop Op) bool {
//	shard := key2shard(oop.Key)
//	gid := kv.needsend[shard]
//	if gid != -1 {
//		return false
//	}
//	return true
//}


//for get and putappend
func (kv *ShardKV) StartCommand(oop Op) (Err, string) {
	//not leader
	_, leader := kv.rf.GetState()
	if leader == false {
		return ErrWrongLeader, ""
	}

	kv.mu.Lock()
	//not responsible for
	if !kv.CheckGroup(oop) {
		kv.mu.Unlock()
		return ErrWrongGroup, ""
	}

	if res, ok := kv.detectDup[oop.ClientId]; ok && res >= oop.Seq {
		resvalue := ""
		if oop.Opname == "Get" {
			resvalue = kv.kvdatabase[oop.Key]
		}
		kv.mu.Unlock()
		return OK, resvalue
	}

	index, _, isLeader := kv.rf.Start(oop)
	if !isLeader {
		kv.mu.Unlock()
		return ErrWrongLeader, ""
	}
	//fmt.Println("index",index, "log op:", oop.Opname, "key: ", oop.Key, "value: ", oop.Value, "cid: ", oop.ClientId, "seq: ", oop.Seq)
	ch := make(chan Op, 1)
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
			if c.err == ErrWrongGroup{
				return ErrWrongGroup, ""
			}
			DPrintf("reply to client: %d\n", index)
			val := ""
			if oop.Opname == "Get" {
				val = c.Value
			}
			return OK, val
		} else {
			return ErrWrongLeader, ""
		}
	case <-time.After(time.Millisecond * 2000):
		DPrintf("timeout index %d\n", index)
		return ErrWrongLeader, ""
	}
}



func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	sl := make([]int, 0)
	op := Op{"Get", args.Key, "",sl, args.ClientId, args.Seq, OK}
	err, val := kv.StartCommand(op)

	reply.Err = err

	if err == ErrWrongLeader {
		reply.WrongLeader = true
		return
	}

	reply.WrongLeader = false
	reply.Value = val
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	sl := make([]int, 0)
	fmt.Println(args.Op, args.Key, args.Value,args.ClientId, args.Seq, kv.me)
	op := Op{args.Op, args.Key, args.Value, sl, args.ClientId, args.Seq, OK}
	err, _ := kv.StartCommand(op)
	reply.Err = err
	if err == ErrWrongLeader {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
	}
}


func (kv *ShardKV) Pull(args *PullArgs, reply *PullReply) {
	pullop := Op{"Pull", "", "", args.Shard, args.ClientId, args.Seq, OK}
	reply.MapKV = make(map[string]string)
	reply.WrongLeader = false
	reply.Err = OK

	//not leader
	_, leader := kv.rf.GetState()
	if leader == false {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	//duplicate
	if res, ok := kv.detectDup[pullop.ClientId]; ok && res >= pullop.Seq {
		for k, v := range kv.kvdatabase {
			shard := key2shard(k)
			for i := 0; i < len(pullop.Shard); i++ {
				if shard == pullop.Shard[i] {
					reply.MapKV[k] = v
					break
				}
			}
		}
		kv.mu.Unlock()
		return
	}

	//not the same config
	if args.Seq != kv.myconfig[0].Num {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	//cannot serve
	for i := 0; i < len(pullop.Shard); i++ {
		if kv.myshards[pullop.Shard[i]] == 0 {
			//cannot serve
			//maybe not in this group
			reply.WrongLeader = true
			reply.Err = ErrWrongLeader
			kv.mu.Unlock()
			return
		}
	}

	index, _, isLeader := kv.rf.Start(pullop)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	//fmt.Println("index",index, "log op:", oop.Opname, "key: ", oop.Key, "value: ", oop.Value, "cid: ", oop.ClientId, "seq: ", oop.Seq)
	ch := make(chan Op, 1)
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
		if kv.CheckSame(c, pullop) {
			kv.mu.Lock()
			for k, v := range kv.kvdatabase {
				shard := key2shard(k)
				for i := 0; i < len(pullop.Shard); i++ {
					if shard == pullop.Shard[i] {
						reply.MapKV[k] = v
						break
					}
				}
			}
			kv.mu.Unlock()
			return
		} else {
			reply.WrongLeader = true
			reply.Err = ErrWrongLeader
			return
		}
	case <-time.After(time.Millisecond * 2000):
		DPrintf("timeout index %d\n", index)
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}
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
	kv.mu.Lock()
	kv.state = Killed
	kv.mu.Unlock()
}

func (kv *ShardKV) DupCheck(cliid int64, seqid int) bool {
	res, ok := kv.detectDup[cliid]
	if ok {
		return seqid > res
	}
	return true
}

func (kv *ShardKV) CheckMigrateDone() bool {
	for i := 0; i < shardmaster.NShards; i++ {
		if kv.needsend[i] != -1  {
			//still in migrate
			return false
		}
	}
	if len(kv.needrecv) == 0 {
		return false
	}
	return true
}

func (kv *ShardKV) GenerateNeedList(newcfg Cfg) {
	for i := 0; i < shardmaster.NShards; i++ {
		if kv.myconfig[0].Shards[i] == kv.gid && newcfg.NewConfig.Shards[i] != kv.gid {
			//need to send
			kv.needsend[i] = newcfg.NewConfig.Shards[i]
		}
		if kv.myconfig[0].Shards[i] != kv.gid && newcfg.NewConfig.Shards[i] == kv.gid {
			//need to recv
			_, ok := kv.needrecv[kv.myconfig[0].Shards[i]]
			if !ok {
				kv.needrecv[kv.myconfig[0].Shards[i]] = make([]int, 0)
			}
			kv.needrecv[kv.myconfig[0].Shards[i]] = append(kv.needrecv[kv.myconfig[0].Shards[i]], i)
		}
	}
}

func (kv *ShardKV) SwitchConfig(newcfg Cfg) {
	if newcfg.NewConfig.Num == kv.myconfig[0].Num + 1 {
		//switch to new config
		//generate need migrate list
		// maintain myshards
		if kv.myconfig[0].Num != 0 {
			kv.GenerateNeedList(newcfg)
		}

		newc := kv.MakeEmptyConfig()
		kv.CopyConfig(&newc, &newcfg.NewConfig)
		kv.myconfig[1] = newc
	}
}

func (kv *ShardKV) MigrateDupCheck(ggid int, num int) bool {
	res, ok := kv.migrateDup[ggid]
	if ok {
		return num > res
	}
	return true
}

func (kv *ShardKV) ApplyOp() {
	for {
		kv.mu.Lock()
		st := kv.state
		kv.mu.Unlock()
		if st == Killed {
			return
		}

		msg := <-kv.applyCh
		if msg.CommandValid {
			index := msg.CommandIndex
			if msg.Command != nil {

				kv.mu.Lock()
				switch cmd := msg.Command.(type) {
				case Op:
					err := OK
					res := Op{}
					if cmd.Opname == "Pull" {
						if kv.DupCheck(cmd.ClientId, cmd.Seq) {
							//will not serve these shard
							for i := 0; i < len(cmd.Shard); i++ {
								kv.myshards[cmd.Shard[i]] = 0
								kv.needsend[cmd.Shard[i]] = -1
							}
							kv.detectDup[cmd.ClientId] = cmd.Seq
							if kv.CheckMigrateDone() {
								//migrate done
								//switch config
								kv.myconfig[0] = kv.myconfig[1]
							}
						}
						sl := make([]int, 0)
						res = Op{cmd.Opname, "","", sl, cmd.ClientId, cmd.Seq, OK}
					} else if cmd.Opname == "Del" {
						//TODO
					} else {
						if !kv.CheckGroup(cmd) {
							err = ErrWrongGroup
						} else {
							if kv.DupCheck(cmd.ClientId, cmd.Seq) {
								switch cmd.Opname {
								case "Put":
									kv.kvdatabase[cmd.Key] = cmd.Value
								case "Append":
									if _, ok := kv.kvdatabase[cmd.Key]; ok{
										kv.kvdatabase[cmd.Key] += cmd.Value
									} else {
										kv.kvdatabase[cmd.Key] = cmd.Value
									}
								}
								kv.detectDup[cmd.ClientId] = cmd.Seq
							}
						}
						sl := make([]int, 0)
						res = Op{cmd.Opname, cmd.Key, kv.kvdatabase[cmd.Key], sl, cmd.ClientId, cmd.Seq, OK}
					}

					if err == ErrWrongGroup {
						res.err = ErrWrongGroup
					}
					ch, ok := kv.chanresult[index]
					if ok {
						select {
						case <- ch:
							default:
						}
						ch <- res
					}

				case Cfg:
					if kv.DupCheck(cmd.ClientId, cmd.Seq) {
						kv.SwitchConfig(cmd)
						kv.detectDup[cmd.ClientId] = cmd.Seq
						if kv.CheckMigrateDone() {
							//migrate done
							//switch config
							kv.myconfig[0] = kv.myconfig[1]
						}
					}
					//switch successful
					//new migrate list
				case Migrate:
					if kv.MigrateDupCheck(cmd.Gid, cmd.Num) {
						for k, v := range cmd.MapKV {
							kv.kvdatabase[k] = v
						}
						for i := 0; i < len(kv.needrecv[cmd.Gid]); i++ {
							kv.myshards[kv.needrecv[cmd.Gid][i]] = 1
						}
						delete(kv.needrecv, cmd.Gid)
					}
				//case Delop:

				}

				if kv.maxraftstate != -1 && kv.rf.GetStateSize() >= kv.maxraftstate && index == kv.rf.GetCommitIndex() {
					kv.SaveSnapshot(index)
				}
				kv.mu.Unlock()
			}
		} else {
			kv.LoadSnapshot(msg.Snapshot)
		}
	}
}

func (kv *ShardKV) UpdateConfig() {
	for {
		kv.mu.Lock()
		st := kv.state
		kv.mu.Unlock()
		if st == Killed {
			return
		}



		kv.mu.Lock()
		//if kv.CheckMigrateDone() {
		//	//switch to new config
		//	//migrate is finish
		//	kv.myconfig[0] = kv.myconfig[1]
		//}

		cur := kv.myconfig[0].Num
		kv.mu.Unlock()

		config := kv.mck.Query(cur+1)
		kv.mu.Lock()
		if config.Num == kv.myconfig[0].Num + 1 {
			//next config
			newcfg := kv.MakeEmptyConfig()
			kv.CopyConfig(&newcfg, &config)

			if _, isLeader := kv.rf.GetState(); isLeader {
				if kv.CheckMigrateDone() {
					kv.rf.Start(Cfg{newcfg, int64(kv.gid), kv.myconfig[0].Num})
				}
			}
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)

	}
}

func (kv *ShardKV) DoMigrate() {
	for {
		kv.mu.Lock()
		st := kv.state
		kv.mu.Unlock()
		if st == Killed {
			return
		}

		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			for k, v := range kv.needrecv {
				//need to send rpc to get shard
				needshard := make([]int, 0)
				for i := 0; i < len(v); i++ {
					needshard = append(needshard, v[i])
				}
				args := &PullArgs{needshard, int64(kv.gid), kv.myconfig[0].Num}
				go func(ggid int, arg *PullArgs){
					servers := kv.myconfig[0].Groups[ggid]
					for {
						for _, si := range servers {
							reply := &PullReply{}
							srv := kv.make_end(si)
							ok := srv.Call("ShardKV.Pull", arg, reply)

							if ok && reply.WrongLeader == false {
								//success pull data
								//apply to my group
								if _, isLeader := kv.rf.GetState(); isLeader {
									newmapkv := make(map[string]string)
									for k, v := range reply.MapKV {
										newmapkv[k] = v
									}
									mig := Migrate{newmapkv, arg.Seq, ggid}
									kv.mu.Lock()
									kv.rf.Start(mig)
									kv.mu.Unlock()
									return
								}
							}
							time.Sleep(10 * time.Millisecond)
						}
					}
				}(k, args)
			}
			kv.mu.Unlock()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) SaveSnapshot(index int) {
	// TODO
}

func (kv *ShardKV) LoadSnapshot(snapshot []byte) {
	// TODO
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
	labgob.Register(Cfg{})
	labgob.Register(Migrate{})
	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.state = Alive
	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.detectDup = make(map[int64]int)
	kv.kvdatabase = make(map[string]string)
	kv.chanresult = make(map[int]chan Op)

	kv.myconfig[0] = kv.mck.Query(0)
	kv.myconfig[1] = kv.myconfig[0]
	kv.needrecv = make(map[int][]int)
	for i := 0; i < shardmaster.NShards; i++ {
		kv.myshards[i] = 0
		kv.needsend[i] = -1
	}
	kv.migrateDup = make(map[int]int)

	kv.LoadSnapshot(persister.ReadRaftState())
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.ApplyOp()
	go kv.UpdateConfig()
	go kv.DoMigrate()

	return kv
}
