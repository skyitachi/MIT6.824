package shardmaster

import (
	"raft"
)
import "labrpc"
import "sync"
import "labgob"
import "time"
import "fmt"

type Result struct {
	Opname string
	ClientId     int64
	Seq int
	config Config
}

const (
	Alive = iota
	Killed
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num
	detectDup map[int64]Result
	result map[int]chan Op

	state   int
}


type Op struct {
	// Your data here.
	Opname  string

	Servers map[int][]string
	GIDs []int
	Shard int
	GID int
	Num int

	ClientId      int64
	Seq     int
}

func (sm *ShardMaster) MakeEmptyConfig() Config{
	res := Config{}
	res.Num = 0;
	for i := 0; i < NShards; i++ {
		res.Shards[i] = 0
	}
	res.Groups = make(map[int][]string)
	return res
}

func (sm *ShardMaster) CopyConfig(c1 *Config, c2 *Config) {
	if c2 == nil {
		return
	}
	c1.Num = c2.Num
	for i := 0; i < NShards; i++ {
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

func (sm *ShardMaster) DupCheck(id int64, seq int) bool {
	res, ok := sm.detectDup[id]
	if ok{
		return seq > res.Seq
	}
	return true
}

func (sm *ShardMaster) CheckSame(c1 Op, c2 Op) bool {
	if c1.ClientId == c2.ClientId && c1.Seq == c2.Seq{
		return true
	}
	return false
}

func (sm *ShardMaster) StartCommand(op Op) (Err, Config){
	sm.mu.Lock()
	if !sm.DupCheck(op.ClientId, op.Seq){
		res, _ := sm.detectDup[op.ClientId]
		resCig := sm.MakeEmptyConfig()
		sm.CopyConfig(&resCig, &res.config)
		sm.mu.Unlock()
		return OK, resCig
	}

	index, _, isLeader := sm.rf.Start(op)
	if !isLeader{
		sm.mu.Unlock()
		resCig := sm.MakeEmptyConfig()
		return ErrWrongLeader, resCig
	}
	//fmt.Println("index",index, sm.me, op)
	ch := make(chan Op, 1)
	sm.result[index] = ch
	sm.mu.Unlock()
	defer func(){
		sm.mu.Lock()
		delete(sm.result, index)
		sm.mu.Unlock()
	}()

	select{
	case c := <-ch:
		if sm.CheckSame(c, op) {
			sm.mu.Lock()
			res := sm.detectDup[op.ClientId]
			resCig := sm.MakeEmptyConfig()
			sm.CopyConfig(&resCig, &res.config)
			sm.mu.Unlock()
			return OK, resCig
		} else{
			resCig := sm.MakeEmptyConfig()
			return ErrWrongLeader, resCig
		}
		//fmt.Println("success ", op.Id, op.Seq)
	case <- time.After(time.Millisecond * 2000):
		fmt.Println("timeout")
		resCig := sm.MakeEmptyConfig()
		return ErrWrongLeader, resCig
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{Opname:"Join",Servers:args.Servers, ClientId:args.ClientId, Seq:args.Seq}
	err, _ := sm.StartCommand(op)

	reply.Err = err
	if err == ErrWrongLeader{
		reply.WrongLeader = true
	}else{
		reply.WrongLeader = false
	}
	if err == OK {
		fmt.Println(sm.configs[len(sm.configs) - 1])
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{Opname:"Leave", GIDs:args.GIDs, ClientId:args.ClientId, Seq:args.Seq}
	err, _ := sm.StartCommand(op)

	reply.Err = err
	if err == ErrWrongLeader{
		reply.WrongLeader = true
	}else{
		reply.WrongLeader = false
	}
	if err == OK {
		fmt.Println(sm.configs[len(sm.configs) - 1])
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{Opname:"Move", Shard:args.Shard, GID:args.GID, ClientId:args.ClientId, Seq:args.Seq}
	err, _ := sm.StartCommand(op)

	reply.Err = err
	if err == ErrWrongLeader{
		reply.WrongLeader = true
	}else{
		reply.WrongLeader = false
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	//if args.Num == -1 || args.Num >= len(sm.configs){
	//	args.Num = len(sm.configs) - 1
	//}
	op := Op{Opname:"Query", Num: args.Num, ClientId:args.ClientId, Seq:args.Seq}
	//fmt.Println(sm.me, "op is ",op)
	err, conf := sm.StartCommand(op)

	reply.Config = conf
	reply.Err = err
	if err == ErrWrongLeader{
		reply.WrongLeader = true
	}else{
		reply.WrongLeader = false
	}
	//fmt.Println(reply.Config)
}

func LoadBalance(config *Config){
	num := len(config.Groups)
	if num == 0{
		return
	}
	every := NShards / num
	mod := NShards % num
	var a int
	for k, _ := range config.Groups{
		a = k
		break
	}
	count := make(map[int]int)
	for i := range config.Shards{
		if config.Shards[i] == 0 {
			count[a] ++ 
			config.Shards[i] = a
		}else{
			count[config.Shards[i]]++
		}
	}
	for i := range config.Groups{
		//fmt.Println(i,count[i])
		for count[i] < every{
			for j := range config.Shards{
				if count[config.Shards[j]] > every{
					count[i]++
					count[config.Shards[j]]--
					config.Shards[j] = i
					break
				}
			}
		}
	}
	//for i := range config.Shards{
	//	fmt.Println(i,config.Shards[i], count[config.Shards[i]])
	//}
	for i := range config.Groups{
		if count[i] >= every + 1{
			mod--
		}
	}
	for i := range config.Groups{
		if mod > 0 && count[i] < every + 1{
			for j := range config.Shards{
				if count[config.Shards[j]] > every + 1{
					count[i]++
					count[config.Shards[j]]--
					config.Shards[j] = i
					break
				}
			}
			mod--
		}
	}
}

func (sm *ShardMaster) Apply(op Op){
	if sm.DupCheck(op.ClientId, op.Seq){
		newConfig := sm.MakeEmptyConfig()
		sm.CopyConfig(&newConfig, &sm.configs[len(sm.configs) - 1])
		newConfig.Num = sm.configs[len(sm.configs) - 1].Num + 1
	
		//fmt.Println(op.Opname)
		switch op.Opname{
		case "Join":
			for k, v := range op.Servers{
				newConfig.Groups[k] = make([]string, len(v))
				for i := 0; i < len(v); i++ {
					newConfig.Groups[k][i] = v[i]
				}
				//newConfig.Groups[k] = v
			}
			LoadBalance(&newConfig)
			//fmt.Println("finish join")
		case "Leave":
			for _, v := range op.GIDs{
				delete(newConfig.Groups, v)
				for j := range newConfig.Shards{
					if newConfig.Shards[j] == v{
						newConfig.Shards[j] = 0
					}
				}
			}
			LoadBalance(&newConfig)
		case "Move":
			newConfig.Shards[op.Shard] = op.GID
		}
		con := sm.MakeEmptyConfig()
		if op.Opname == "Query" {
			num := op.Num
			if op.Num == -1 || op.Num >= len(sm.configs) {
				num = len(sm.configs) - 1
			}
			sm.CopyConfig(&con, &sm.configs[num])
			//con = sm.configs[num]
		} else {
			sm.configs = append(sm.configs, newConfig)
		}
		sm.detectDup[op.ClientId] = Result{op.Opname, op.ClientId, op.Seq, con}

	}
}

func (sm *ShardMaster) Reply(op Op, index int){
	ch, ok := sm.result[index]
	if ok{
		select{
		case <- ch:
		default:
		}
		ch <- op
	}
}

func (sm *ShardMaster) doApplyOp(){
	for{
		//Killed
		sm.mu.Lock()
		st := sm.state
		sm.mu.Unlock()
		if st == Killed {
			return
		}

		msg := <-sm.applyCh
		index := msg.CommandIndex
		if op, ok := msg.Command.(Op); ok{
			sm.mu.Lock()
			sm.Apply(op)
			sm.Reply(op, index)
			sm.mu.Unlock()
		}
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	sm.mu.Lock()
	sm.state = Killed
	sm.mu.Unlock()
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me
	sm.state = Alive
	sm.configs = make([]Config, 1)
	sm.configs[0] = sm.MakeEmptyConfig()

	labgob.Register(Op{})
	sm.detectDup = make(map[int64]Result)
	sm.result = make(map[int]chan Op)

	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.

	go sm.doApplyOp()
	return sm
}
