package shardmaster


import "raft"
import "labrpc"
import "sync"
import "labgob"
import "time"
import "fmt"

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num
	DetectDup map[int64]int
	result map[int]chan Op
}


type Op struct {
	// Your data here.
	Opname  string

	Servers map[int][]string
	GIDs []int
	Shard int
	GID int

	Id      int64
	Seq     int
}

func (sm *ShardMaster) DupCheck(id int64, seq int) bool {
	ssq, ok := sm.DetectDup[id]
	if ok{
		return seq > ssq
	}
	return true
}

func (sm *ShardMaster) StartCommand(op Op) Err{
	sm.mu.Lock()
	if !sm.DupCheck(op.Id, op.Seq){
		sm.mu.Unlock()
		return OK
	}

	index, _, isLeader := sm.rf.Start(op)
	if !isLeader{
		sm.mu.Unlock()
		return ErrWrongLeader
	}
	//fmt.Println("index",index, sm.me, op)
	ch := make(chan Op)
	sm.result[index] = ch
	sm.mu.Unlock()
	defer func(){
		sm.mu.Lock()
		delete(sm.result, index)
		sm.mu.Unlock()
	}()

	select{
	case <-ch:
		//fmt.Println("success ", op.Id, op.Seq)
		return OK
	case <- time.After(time.Millisecond * 2000):
		fmt.Println("timeout")
		return ErrTimeout
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{Opname:"Join",Servers:args.Servers, Id:args.Id, Seq:args.Seq}
	err := sm.StartCommand(op)

	reply.Err = err
	if err != OK{
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
	op := Op{Opname:"Leave", GIDs:args.GIDs, Id:args.Id, Seq:args.Seq}
	err := sm.StartCommand(op)

	reply.Err = err
	if err != OK{
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
	op := Op{Opname:"Move", Shard:args.Shard, GID:args.GID, Id:args.Id, Seq:args.Seq}
	err := sm.StartCommand(op)

	reply.Err = err
	if err != OK{
		reply.WrongLeader = true
	}else{
		reply.WrongLeader = false
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	if args.Num == -1 || args.Num >= len(sm.configs){
		args.Num = len(sm.configs) - 1
	}
	op := Op{Opname:"Query", Id:args.Id, Seq:args.Seq}
	//fmt.Println(sm.me, "op is ",op)
	err := sm.StartCommand(op)

	reply.Err = err
	if err != OK{
		reply.WrongLeader = true
	}else{
		reply.WrongLeader = false
	}
	sm.mu.Lock()
	reply.Config = sm.configs[args.Num]
	sm.mu.Unlock()
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
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.DupCheck(op.Id, op.Seq){
		newConfig := Config{}
		newConfig.Num = sm.configs[len(sm.configs) - 1].Num + 1
		for index, shard := range sm.configs[len(sm.configs)-1].Shards{
			newConfig.Shards[index] = shard
		}
		newConfig.Groups = make(map[int][]string)
		for key, value := range sm.configs[len(sm.configs)-1].Groups{
			newConfig.Groups[key] = value
		}
		//fmt.Println(op.Opname)
		switch op.Opname{
		case "Join":
			for k, v := range op.Servers{
				newConfig.Groups[k] = v
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
		sm.DetectDup[op.Id] = op.Seq
		sm.configs = append(sm.configs, newConfig)
	}
}

func (sm *ShardMaster) Reply(op Op, index int){
	sm.mu.Lock()
	ch, ok := sm.result[index]
	sm.mu.Unlock()
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
		msg := <-sm.applyCh
		index := msg.CommandIndex
		if op, ok := msg.Command.(Op); ok{
			if op.Opname != "Query"{
				sm.Apply(op)
			}
			sm.Reply(op, index)
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

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.DetectDup = make(map[int64]int)
	sm.result = make(map[int]chan Op)

	go sm.doApplyOp()
	return sm
}
