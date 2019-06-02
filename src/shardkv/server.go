package shardkv


// import "shardmaster"
import (
	"bytes"
	"fmt"
	"labrpc"
	"log"
	"shardmaster"
	"sort"
	"time"
)
import "raft"
import "sync"
import "labgob"

const Debug = 1

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

	Error    Err
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
	ShardDup    [shardmaster.NShards]map[int64]int

	Num   int
	Gid   int
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

	detectDup	 [shardmaster.NShards]map[int64]int
	pullDup	     map[int64]int
	cfgDup		 map[int64]int
	migrateDup   map[int]int
	delDup		 map[int64]int

	kvdatabase   map[string]string
	chanresult   map[int]chan Op
	chandel	     map[int]chan Op

	myconfig     [2]shardmaster.Config //0 is now, 1 is new // 0 + need is current state
	//when recv from other group and apply to my group, the shard is in my group
	//when others query from me, the shard is not in my group, cannot serve
	myshards	 [shardmaster.NShards]int //shard -> 0/1
	needrecv     map[int][]int 			  // gid->[]shard
	needsend     [shardmaster.NShards]int //shard->gid/-1
	needdel		 map[int]map[int][]int //shard->gid/-1
	delconfig    map[int]map[int][]string //gid->servers
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
		DPrintf("this group %d-%d is not leader", kv.gid, kv.me)
		return ErrWrongLeader, ""
	}
	DPrintf("group %d-%d attempt to deal with op %d %d", kv.gid, kv.me, oop.ClientId, oop.Seq)
	kv.mu.Lock()
	//not responsible for
	if !kv.CheckGroup(oop) {
		kv.mu.Unlock()
		DPrintf("this group %d-%d not responsible for the key %s, shard is %d, my shards is %v", kv.gid, kv.me, oop.Key, key2shard(oop.Key), kv.myshards)
		return ErrWrongGroup, ""
	}

	if res, ok := kv.detectDup[key2shard(oop.Key)][oop.ClientId]; ok && res >= oop.Seq {
		resvalue := ""
		if oop.Opname == "Get" {
			resvalue = kv.kvdatabase[oop.Key]
		}
		kv.mu.Unlock()
		DPrintf("this group %d-%d duplicate for the key %s", kv.gid, kv.me, oop.Key)
		return OK, resvalue
	}

	index, _, isLeader := kv.rf.Start(oop)
	if !isLeader {
		kv.mu.Unlock()
		DPrintf("after start, this group %d-%d is not leader", kv.gid, kv.me)
		return ErrWrongLeader, ""
	}
	DPrintf("group %d-%d wait raft to apply %d %d", kv.gid, kv.me, oop.ClientId, oop.Seq)
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
			if c.Error == ErrWrongGroup{
				return ErrWrongGroup, ""
			}
			DPrintf("group %d reply to client: %d, seq %d", kv.gid, c.ClientId, c.Seq)
			val := ""
			if oop.Opname == "Get" {
				val = c.Value
			}
			return OK, val
		} else {
			return ErrWrongLeader, ""
		}
	case <-time.After(time.Millisecond * 2000):
		DPrintf("group %d timeout index %d",kv.gid, index)
		return ErrWrongLeader, ""
	}
}



func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	sl := make([]int, 0)
	DPrintf("Get from client %d %d, key %s, to group %d-%d", args.ClientId, args.Seq, args.Key, kv.gid, kv.me)
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
	DPrintf("%s from client %d %d, key %s, value %s, to group %d-%d", args.Op, args.ClientId, args.Seq, args.Key, args.Value, kv.gid, kv.me)
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
	for i := 0; i < shardmaster.NShards; i++ {
		reply.ShardDup[i] = make(map[int64]int)
	}
	reply.WrongLeader = false
	reply.Err = OK

	//not leader
	_, leader := kv.rf.GetState()
	if leader == false {
		DPrintf("pullop: group %d-%d is not leader, shard %v, cli %d, config %d", kv.gid, kv.me, args.Shard, args.ClientId, args.Seq)
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	//duplicate
	if res, ok := kv.pullDup[pullop.ClientId]; ok && res >= pullop.Seq {
		for k, v := range kv.kvdatabase {
			shard := key2shard(k)
			for i := 0; i < len(pullop.Shard); i++ {
				if shard == pullop.Shard[i] {
					reply.MapKV[k] = v
					break
				}
			}
		}
		for i := 0; i < len(pullop.Shard); i++ {
			for k, v := range kv.detectDup[pullop.Shard[i]] {
				reply.ShardDup[pullop.Shard[i]][k] = v
			}
		}
		DPrintf("pullop: group %d-%d duplicate, shard %v, cli %d, config %d", kv.gid, kv.me, args.Shard, args.ClientId, args.Seq)
		kv.mu.Unlock()
		return
	}

	//not the same config
	if args.Seq != kv.myconfig[0].Num {
		reply.WrongLeader = false
		reply.Err = ErrNeedWait
		DPrintf("pullop: group %d-%d not the same config, myconfig %d, shard %v, cli %d, config %d", kv.gid, kv.me, kv.myconfig[0].Num,args.Shard, args.ClientId, args.Seq)
		kv.mu.Unlock()
		return
	}

	//cannot serve
	for i := 0; i < len(pullop.Shard); i++ {
		if kv.myshards[pullop.Shard[i]] == 0 || kv.needsend[pullop.Shard[i]] == -1 {
			//cannot serve
			//maybe not in this group
			reply.WrongLeader = false
			reply.Err = ErrNeedWait
			DPrintf("pullop: group %d-%d is not serve shard %v, cli %d, config %d, myshard %v, mysend %v", kv.gid, kv.me, args.Shard, args.ClientId, args.Seq, kv.myshards, kv.needsend)
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
			for i := 0; i < len(pullop.Shard); i++ {
				for k, v := range kv.detectDup[pullop.Shard[i]] {
					reply.ShardDup[pullop.Shard[i]][k] = v
				}
			}
			DPrintf("pullop: group %d-%d pull successful, shard %v, cli %d, config %d", kv.gid, kv.me, args.Shard, args.ClientId, args.Seq)
			kv.mu.Unlock()
			return
		} else {
			reply.WrongLeader = true
			reply.Err = ErrWrongLeader
			return
		}
	case <-time.After(time.Millisecond * 2000):
		DPrintf("timeout index %d", index)
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}
}

func (kv *ShardKV) Del(args *DelArgs, reply *DelReply) {
	delop := Op{"Del", "", "", args.Shard, args.ClientId, args.Seq, OK}
	reply.WrongLeader = false
	reply.Err = OK

	//not leader
	_, leader := kv.rf.GetState()
	if leader == false {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		DPrintf("delop: group %d-%d is not leader, shard %v, cli %d, config %d", kv.gid, kv.me, args.Shard, args.ClientId, args.Seq)
		return
	}

	kv.mu.Lock()
	//duplicate
	if res, ok := kv.delDup[delop.ClientId]; ok && res >= delop.Seq {
		DPrintf("delop: group %d-%d duplicate, has delete, shard %v, cli %d, config %d", kv.gid, kv.me, args.Shard, args.ClientId, args.Seq)
		kv.mu.Unlock()
		return
	}

	//delete future shard
	if args.Seq > kv.myconfig[0].Num {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		DPrintf("delop: group %d-%d config is old %d, has delete, shard %v, cli %d, config %d", kv.gid, kv.me, kv.myconfig[0].Num, args.Shard, args.ClientId, args.Seq)
		kv.mu.Unlock()
		return
	}

	////be serving this shard, cannot delete
	//for i := 0; i < len(delop.Shard); i++ {
	//	if kv.myshards[delop.Shard[i]] != 0 {
	//		// this shard is still in this group
	//		// cannot delete
	//		// can return OK
	//		DPrintf("delop: group %d-%d duplicate, has delete, shard %v, cli %d, config %d", kv.gid, kv.me, args.Shard, args.ClientId, args.Seq)
	//		kv.mu.Unlock()
	//		return
	//	}
	//}

	index, _, isLeader := kv.rf.Start(delop)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	//fmt.Println("index",index, "log op:", oop.Opname, "key: ", oop.Key, "value: ", oop.Value, "cid: ", oop.ClientId, "seq: ", oop.Seq)
	ch := make(chan Op, 1)
	kv.chandel[index] = ch
	kv.mu.Unlock()
	defer func() {
		kv.mu.Lock()
		delete(kv.chandel, index)
		kv.mu.Unlock()
	}()
	//fmt.Println("unlock")
	select {
	case c := <-ch:
		if kv.CheckSame(c, delop) {
			DPrintf("delop: group %d-%d delete successful, shard %v, cli %d, config %d", kv.gid, kv.me, args.Shard, args.ClientId, args.Seq)
			return
		} else {
			reply.WrongLeader = true
			reply.Err = ErrWrongLeader
			return
		}
	case <-time.After(time.Millisecond * 2000):
		DPrintf("timeout index %d", index)
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

func (kv *ShardKV) DupCheck(cliid int64, seqid int, key string) bool {
	res, ok := kv.detectDup[key2shard(key)][cliid]
	if ok {
		return seqid > res
	}
	return true
}

func (kv *ShardKV) PullDupCheck(cliid int64, seqid int) bool {
	res, ok := kv.pullDup[cliid]
	if ok {
		return seqid > res
	}
	return true
}

func (kv *ShardKV) CfgDupCheck(cliid int64, seqid int) bool {
	res, ok := kv.cfgDup[cliid]
	if ok {
		return seqid > res
	}
	return true
}

func (kv *ShardKV) MigrateDupCheck(ggid int, num int) bool {
	res, ok := kv.migrateDup[ggid]
	if ok {
		return num > res
	}
	return true
}

func (kv *ShardKV) DelDupCheck(cliid int64, seqid int) bool {
	res, ok := kv.delDup[cliid]
	if ok {
		return seqid > res
	}
	return true
}


func (kv *ShardKV) CheckMigrateDone() bool {
	for i := 0; i < shardmaster.NShards; i++ {
		if kv.needsend[i] != -1  {
			//still in migrate
			DPrintf("group %d-%d needsend is not empty", kv.gid, kv.me)
			return false
		}
	}
	if len(kv.needrecv) != 0 {
		DPrintf("group %d-%d needrecv is not empty",kv.gid, kv.me)
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
	DPrintf("group %d-%d generate need list %d to %d", kv.gid, kv.me, kv.myconfig[0].Num, newcfg.NewConfig.Num)
}

func (kv *ShardKV) SwitchConfig(newcfg Cfg) {
	if newcfg.NewConfig.Num == kv.myconfig[0].Num + 1 {
		//switch to new config
		//generate need migrate list
		// maintain myshards
		DPrintf("group %d-%d switchconfig from %d to %d", kv.gid, kv.me, kv.myconfig[0].Num, newcfg.NewConfig.Num)
		if kv.myconfig[0].Num != 0 {
			kv.GenerateNeedList(newcfg)
		} else if kv.myconfig[0].Num == 0{
			for i := 0; i < shardmaster.NShards; i++ {
				if newcfg.NewConfig.Shards[i] == kv.gid {
					kv.myshards[i] = 1
				}
			}
		}

		newc := kv.MakeEmptyConfig()
		kv.CopyConfig(&newc, &newcfg.NewConfig)
		kv.myconfig[1] = newc
	}
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
						if kv.PullDupCheck(cmd.ClientId, cmd.Seq) {
							//will not serve these shard
							for i := 0; i < len(cmd.Shard); i++ {
								kv.myshards[cmd.Shard[i]] = 0
								kv.needsend[cmd.Shard[i]] = -1
							}
							if kv.CheckMigrateDone() {
								//migrate done
								//switch config
								kv.myconfig[0] = kv.myconfig[1]
								DPrintf("Pull: group %d-%d successful switch to config %d", kv.gid, kv.me, kv.myconfig[0].Num)
							}
							kv.pullDup[cmd.ClientId] = cmd.Seq
						}
						sl := make([]int, 0)
						res = Op{cmd.Opname, "","", sl, cmd.ClientId, cmd.Seq, OK}
					} else if cmd.Opname == "Del" {
						//be serving this shard, cannot delete
						//flag := 0
						//for i := 0; i < len(cmd.Shard); i++ {
						//	if kv.myshards[cmd.Shard[i]] != 0 {
						//		// this shard is still in this group
						//		// cannot delete
						//		// can return OK
						//		DPrintf("group %d-%d now is serving the shard %d, now config num %d, delop num %d", kv.gid, kv.me, cmd.Shard[i], kv.myconfig[0].Num, cmd.Seq)
						//		flag = 1
						//	}
						//}

						//if flag == 0 && kv.DelDupCheck(cmd.ClientId, cmd.Seq) {
						if kv.DelDupCheck(cmd.ClientId, cmd.Seq) {
							for k, _:= range kv.kvdatabase {
								for i := 0; i < len(cmd.Shard); i++ {
									if key2shard(k) == cmd.Shard[i] && kv.myshards[cmd.Shard[i]] == 0{
										//this shard is not serving, can delete
										delete(kv.kvdatabase, k)
										break
									}
								}
							}
							DPrintf("group %d-%d delete %v successful, now shard is in %d", kv.gid, kv.me, cmd.Shard, cmd.ClientId)
							kv.delDup[cmd.ClientId] = cmd.Seq
						}
						sl := make([]int, 0)
						res = Op{cmd.Opname, "","", sl, cmd.ClientId, cmd.Seq, OK}

					} else {
						if !kv.CheckGroup(cmd) {
							err = ErrWrongGroup
						} else {
							if kv.DupCheck(cmd.ClientId, cmd.Seq, cmd.Key) {
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
								kv.detectDup[key2shard(cmd.Key)][cmd.ClientId] = cmd.Seq
							}
						}
						sl := make([]int, 0)
						res = Op{cmd.Opname, cmd.Key, kv.kvdatabase[cmd.Key], sl, cmd.ClientId, cmd.Seq, OK}
					}

					if err == ErrWrongGroup {
						res.Error = ErrWrongGroup
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
					if kv.CfgDupCheck(cmd.ClientId, cmd.Seq) {
						kv.SwitchConfig(cmd)
						if kv.CheckMigrateDone() {
							//migrate done
							//switch config
							kv.myconfig[0] = kv.myconfig[1]
							DPrintf("Cfg: group %d-%d successful switch to config %d", kv.gid, kv.me, kv.myconfig[0].Num)
						}
						kv.cfgDup[cmd.ClientId] = cmd.Seq

						//if kv.maxraftstate != -1 {
						//	kv.SaveSnapshot(index)
						//}
					}
					//switch successful
					//new migrate list
				case Migrate:
					if kv.MigrateDupCheck(cmd.Gid, cmd.Num) {
						DPrintf("group %d-%d apply the migrate data from %d and config num %d", kv.gid,kv.me,cmd.Gid, cmd.Num)
						for k, v := range cmd.MapKV {
							kv.kvdatabase[k] = v
						}
						for i := 0; i < shardmaster.NShards; i++ {
							for k, v := range cmd.ShardDup[i] {
								kv.detectDup[i][k] = v
							}
						}
						for i := 0; i < len(kv.needrecv[cmd.Gid]); i++ {
							kv.myshards[kv.needrecv[cmd.Gid][i]] = 1
						}
						if _, ok := kv.needdel[kv.myconfig[0].Num]; !ok {
							kv.needdel[kv.myconfig[0].Num] = make(map[int][]int)
						}
						if _, ok := kv.needdel[kv.myconfig[0].Num][cmd.Gid]; !ok {
							kv.needdel[kv.myconfig[0].Num][cmd.Gid] = make([]int, 0)
						}
						for i := 0; i < len(kv.needrecv[cmd.Gid]); i++ {
							kv.needdel[kv.myconfig[0].Num][cmd.Gid] = append(kv.needdel[kv.myconfig[0].Num][cmd.Gid], kv.needrecv[cmd.Gid][i])
						}

						if _, ok := kv.delconfig[kv.myconfig[0].Num]; !ok {
							kv.delconfig[kv.myconfig[0].Num] = make(map[int][]string)
						}
						kv.delconfig[kv.myconfig[0].Num][cmd.Gid] = kv.myconfig[0].Groups[cmd.Gid]
						delete(kv.needrecv, cmd.Gid)
						if kv.CheckMigrateDone() {
							//migrate done
							//switch config
							kv.myconfig[0] = kv.myconfig[1]
							DPrintf("Migrate: group %d-%d successful switch to config %d", kv.gid, kv.me, kv.myconfig[0].Num)
						}
						kv.migrateDup[cmd.Gid] = cmd.Num
						if kv.maxraftstate != -1 {
							kv.SaveSnapshot(index)
						}
					}

				}
				if kv.maxraftstate != -1 && kv.rf.GetStateSize() >= kv.maxraftstate {
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
		DPrintf("group %d-%d now config num %d, get new config num %d, now shard %v, new shard %v", kv.gid, kv.me, kv.myconfig[0].Num, config.Num, kv.myconfig[0].Shards, config.Shards)
		kv.mu.Lock()
		if config.Num == kv.myconfig[0].Num + 1 {
			//next config
			newcfg := kv.MakeEmptyConfig()
			kv.CopyConfig(&newcfg, &config)

			if _, isLeader := kv.rf.GetState(); isLeader {
				if kv.CheckMigrateDone() {
					kv.rf.Start(Cfg{newcfg, int64(kv.gid), kv.myconfig[0].Num})
					DPrintf("group %d-%d start a new config %d, shards %v", kv.gid, kv.me, newcfg.Num, newcfg.Shards)
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
							DPrintf("group %d-%d will pull data %v from group %d", kv.gid, kv.me, arg.Shard, ggid)
							ok := srv.Call("ShardKV.Pull", arg, reply)

							if ok && reply.WrongLeader == false {
								//success pull data
								//apply to my group
								if reply.Err == ErrNeedWait {
									// peer group still in old config
									return
								}
								if _, isLeader := kv.rf.GetState(); isLeader {
									DPrintf("group %d-%d get the pulled data from %d, will migrate", kv.gid, kv.me, ggid)
									newmapkv := make(map[string]string)
									for k, v := range reply.MapKV {
										newmapkv[k] = v
									}
									var newdup [shardmaster.NShards]map[int64]int
									for i := 0; i < shardmaster.NShards; i++ {
										newdup[i] = make(map[int64]int)
										for k, v := range reply.ShardDup[i] {
											newdup[i][k] = v
										}
									}
									mig := Migrate{newmapkv, newdup, arg.Seq, ggid}

									kv.mu.Lock()
									kv.rf.Start(mig)
									DPrintf("group %d-%d start migrate the data pulled from %d", kv.gid, kv.me, ggid)
									kv.mu.Unlock()
									return
								}
							}
							time.Sleep(20 * time.Millisecond)
						}
					}
				}(k, args)
			}
			kv.mu.Unlock()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) DoDelete() {
	for {
		kv.mu.Lock()
		st := kv.state
		kv.mu.Unlock()
		if st == Killed {
			return
		}

		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			grp := make([]int, 0)
			for num := range kv.needdel {
				grp = append(grp, num)
			}
			sort.Ints(grp)
			for _, num := range grp {
				v := kv.needdel[num]
				if len(v) == 0 {
					DPrintf("1. group %d-%d have finish delete work in config %d", kv.gid, kv.me, num)
					delete(kv.needdel, num)
					delete(kv.delconfig, num)
					DPrintf("2. group %d-%d have finish delete work in config %d", kv.gid, kv.me, num)
				}
			}
			for _, num := range grp {
				v := kv.needdel[num]
				for g, s := range v {
					sl := make([]int, 0)
					for i := 0; i < len(s); i++ {
						sl = append(sl, s[i])
					}
					args := &DelArgs{sl, int64(kv.gid), num}
					servers := kv.delconfig[num][g]
					go func(ggid int, serv []string, arg *DelArgs) {
						for {
							for _, si := range serv {
								reply := &DelReply{}
								srv := kv.make_end(si)
								DPrintf("group %d-%d will delete data %v in group %d", kv.gid, kv.me, arg.Shard, ggid)
								ok := srv.Call("ShardKV.Del", arg, reply)

								if ok && reply.WrongLeader == false {
									kv.mu.Lock()
									DPrintf("1. group %d-%d delete data successful in %d", kv.gid, kv.me, ggid)
									delete(kv.needdel[num], ggid)
									DPrintf("2. group %d-%d delete data successful in %d", kv.gid, kv.me, ggid)
									kv.mu.Unlock()
									return
								}
								time.Sleep(20 * time.Millisecond)
							}
						}

					}(g, servers, args)
				}
			}
			kv.mu.Unlock()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) SaveSnapshot(index int) {
	DPrintf("group %d-%d savesnapshot at index %d, config num %d", kv.gid, kv.me, index, kv.myconfig[0].Num)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if ok := e.Encode(kv.kvdatabase); ok != nil  {
		DPrintf("save kvdatabase fail snapshot, %v", ok)
	}
	if ok := e.Encode(kv.detectDup); ok != nil {
		DPrintf("save detectDup fail snapshot, %v", ok)
	}
	if ok := e.Encode(kv.pullDup); ok != nil {
		DPrintf("save pulldup fail snapshot, %v", ok)
	}
	if ok := e.Encode(kv.cfgDup); ok != nil {
		DPrintf("save cfgdup fail snapshot, %v", ok)
	}
	if ok := e.Encode(kv.delDup); ok != nil {
		DPrintf("save deldup fail snapshot, %v", ok)
	}
	if ok := e.Encode(kv.myconfig); ok != nil {
		DPrintf("save myconfig fail snapshot, %v", ok)
	}
	if ok := e.Encode(kv.myshards); ok != nil {
		DPrintf("save myshard fail snapshot, %v", ok)
	}
	if ok := e.Encode(kv.needrecv); ok != nil {
		DPrintf("save needrecv fail snapshot, %v", ok)
	}
	if ok := e.Encode(kv.needsend); ok != nil {
		DPrintf("save needsend fail snapshot, %v", ok)
	}
	if ok := e.Encode(kv.needdel); ok != nil {
		DPrintf("save needdel fail snapshot, %v", ok)
	}
	if ok := e.Encode(kv.migrateDup); ok != nil {
		DPrintf("save migratedup fail snapshot, %v", ok)
	}
	if ok := e.Encode(kv.delconfig); ok != nil {
		DPrintf("save delconfig fail snapshot, %v", ok)
	}
	data := w.Bytes()
	kv.rf.SaveSnapshotlab4(index, data)
}

func (kv *ShardKV) LoadSnapshot(snapshot []byte) {
	// TODO
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	s := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(s)


	var kvdb map[string]string
	var dup [shardmaster.NShards]map[int64]int
	var pulldup map[int64]int
	var cfgdup map[int64]int
	var deldup map[int64]int
	var cfg [2]shardmaster.Config
	var shard [shardmaster.NShards]int
	var recv map[int][]int
	var send [shardmaster.NShards]int
	var del map[int]map[int][]int
	var migdup map[int]int
	var delcfg map[int]map[int][]string
	//if d.Decode(&kvdb) != nil || d.Decode(&dup) != nil || d.Decode(&cfg) != nil || d.Decode(&shard) != nil || d.Decode(&recv) != nil || d.Decode(&send) != nil || d.Decode(&migdup) != nil{
	//	fmt.Println("server ", kv.me, " readsnapshot wrong!")
	//} else {
	//	kv.mu.Lock()
	//	kv.kvdatabase = kvdb
	//	kv.detectDup = dup
	//	kv.myconfig = cfg
	//	kv.myshards = shard
	//	kv.needrecv = recv
	//	kv.needsend = send
	//	kv.migrateDup = migdup
	//	kv.mu.Unlock()
	//	fmt.Println(kv.me, "loadsnapshot")
	//}
	if ok := d.Decode(&kvdb); ok != nil {
		DPrintf("readsnapshot fail, kvdatabase %v", ok)
	}
	if ok := d.Decode(&dup); ok != nil {
		DPrintf("readsnapshot fail, detectdup %v", ok)
	}
	if ok := d.Decode(&pulldup); ok != nil {
		DPrintf("readsnapshot fail, pulldup %v", ok)
	}
	if ok := d.Decode(&cfgdup); ok != nil {
		DPrintf("readsnapshot fail, cfgdup %v", ok)
	}
	if ok := d.Decode(&deldup); ok != nil {
		DPrintf("readsnapshot fail, cfgdup %v", ok)
	}
	if ok := d.Decode(&cfg); ok != nil {
		DPrintf("readsnapshot fail, myconfig %v", ok)
	}
	if ok := d.Decode(&shard); ok != nil {
		DPrintf("readsnapshot fail, myshards %v", ok)
	}
	if ok := d.Decode(&recv); ok != nil {
		DPrintf("readsnapshot fail, needrecv %v", ok)
	}
	if ok := d.Decode(&send); ok != nil {
		DPrintf("readsnapshot fail, needsend %v", ok)
	}
	if ok := d.Decode(&del); ok != nil {
		DPrintf("readsnapshot fail, needdel %v", ok)
	}
	if ok := d.Decode(&migdup); ok != nil {
		DPrintf("readsnapshot fail, migratedup %v", ok)
	}
	if ok := d.Decode(&delcfg); ok != nil {
		DPrintf("readsnapshot fail, delcfg %v", ok)
	}
	kv.mu.Lock()
	kv.kvdatabase = kvdb
	kv.detectDup = dup
	kv.pullDup = pulldup
	kv.cfgDup = cfgdup
	kv.delDup = deldup
	kv.myconfig = cfg
	kv.myshards = shard
	kv.needrecv = recv
	kv.needsend = send
	kv.needdel = del
	kv.migrateDup = migdup
	kv.delconfig = delcfg
	kv.mu.Unlock()
	DPrintf("group %d-%d loadsnapshot", kv.gid, kv.me)
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

	for i := 0; i < shardmaster.NShards; i++ {
		kv.detectDup[i] = make(map[int64]int)
	}
	kv.pullDup = make(map[int64]int)
	kv.cfgDup = make(map[int64]int)
	kv.delDup = make(map[int64]int)
	kv.kvdatabase = make(map[string]string)
	kv.chanresult = make(map[int]chan Op)
	kv.chandel = make(map[int]chan Op)

	kv.myconfig[0] = kv.MakeEmptyConfig()
	kv.myconfig[1] = kv.MakeEmptyConfig()
	kv.needrecv = make(map[int][]int)
	for i := 0; i < shardmaster.NShards; i++ {
		kv.myshards[i] = 0
		kv.needsend[i] = -1
	}
	kv.needdel = make(map[int]map[int][]int)
	kv.delconfig = make(map[int]map[int][]string)
	kv.migrateDup = make(map[int]int)

	kv.LoadSnapshot(persister.ReadSnapshot())
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.ApplyOp()
	go kv.UpdateConfig()
	go kv.DoMigrate()
	go kv.DoDelete()
	return kv
}
