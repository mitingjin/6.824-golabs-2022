package shardctrler

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"sort"
	"sync"
	"time"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num

	maxraftstate int

	done        map[int]chan int64
	lastRequest map[int64]int64

	lastApplied int
}

func (sc *ShardCtrler) persistentStateL() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(sc.configs)
	e.Encode(sc.lastRequest)
	return w.Bytes()
}

func (sc *ShardCtrler) readPersistL(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var configs []Config
	var lastRequest map[int64]int64
	if d.Decode(&configs) != nil || d.Decode(&lastRequest) != nil {

	} else {
		sc.configs = configs
		sc.lastRequest = lastRequest
	}
}

type Op struct {
	// Your data here.
	JoinServers map[int][]string
	LeaveGIDs   []int
	MoveShard   int
	MoveGID     int
	QueryNum    int

	Op        int
	ClientId  int64
	RequestId int64
}

const (
	Join = iota
	Leave
	Move
	Query
)

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()
	op := Op{
		JoinServers: args.Servers,
		Op:          Join,
		ClientId:    args.ClientId,
		RequestId:   args.RequestId,
	}
	if ok := sc.waitForAppliedL(op); ok {
		reply.Err = OK
	} else {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()
	op := Op{
		LeaveGIDs: args.GIDs,
		Op:        Leave,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	if ok := sc.waitForAppliedL(op); ok {
		reply.Err = OK
	} else {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()
	op := Op{
		MoveShard: args.Shard,
		MoveGID:   args.GID,
		Op:        Move,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	if ok := sc.waitForAppliedL(op); ok {
		reply.Err = OK
	} else {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()
	op := Op{
		QueryNum:  args.Num,
		Op:        Query,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	if ok := sc.waitForAppliedL(op); ok {
		reply.Err = OK
		reply.Config = sc.getConfigL(args.Num)
	} else {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) waitForAppliedL(op Op) bool {
	if _, isLeader := sc.rf.GetState(); isLeader {
		index, _, _ := sc.rf.Start(op)
		if _, ok := sc.done[index]; !ok {
			sc.done[index] = make(chan int64)
		}
		ch := sc.done[index]
		sc.mu.Unlock()
		defer sc.mu.Lock()
		select {
		case rid := <-ch:
			if rid == op.RequestId {
				return true
			}
		case <-time.After(1 * time.Second):
		}
	}
	return false
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) applier() {
	for msg := range sc.applyCh {
		sc.mu.Lock()
		if msg.CommandValid {
			rid := sc.applyL(msg.Command.(Op))
			if msg.CommandIndex > sc.lastApplied {
				sc.lastApplied = msg.CommandIndex
			}
			if _, isLeader := sc.rf.GetState(); isLeader {
				ch := sc.done[msg.CommandIndex]
				sc.mu.Unlock()
				select {
				case ch <- rid:
				default:
				}
				sc.mu.Lock()
			}
			if sc.maxraftstate != -1 && sc.rf.RaftStateSize() >= sc.maxraftstate {
				sc.rf.Snapshot(sc.lastApplied, sc.persistentStateL())
			}
		} else {
			sc.readPersistL(msg.Snapshot)
			sc.lastApplied = msg.SnapshotIndex
		}
		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) applyL(op Op) (rid int64) {
	rid = op.RequestId
	if sc.lastRequest[op.ClientId] < op.RequestId {
		config := func() Config {
			lastConfig := sc.getConfigL(-1)
			config := Config{
				Num:    lastConfig.Num + 1,
				Groups: make(map[int][]string),
			}
			for k, v := range lastConfig.Groups {
				config.Groups[k] = append([]string(nil), v...)
			}
			return config
		}()
		switch op.Op {
		case Join:
			for gid, group := range op.JoinServers {
				config.Groups[gid] = append([]string(nil), group...)
			}
		case Leave:
			for _, gid := range op.LeaveGIDs {
				delete(config.Groups, gid)
			}
		case Move:
			config.Shards[op.MoveShard] = op.MoveGID
		case Query:
			return
		}
		sc.configs = append(sc.configs, config)
		sc.lastRequest[op.ClientId] = op.RequestId
		sc.balanceL()
	}
	return
}

func (sc *ShardCtrler) getConfigL(number int) Config {
	if number < 0 || number > len(sc.configs)-1 {
		return sc.configs[len(sc.configs)-1]
	}
	return sc.configs[number]
}

func (sc *ShardCtrler) balanceL() {
	config := &sc.configs[len(sc.configs)-1]
	if len(config.Groups) == 0 {
		return
	}
	gids := make([]int, 0)
	for gid := range config.Groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	unassigned := make([]int, 0)
	for shard := range config.Shards {
		unassigned = append(unassigned, shard)
	}
	if assigned, averageShardCount := 0, len(config.Shards)/len(gids); assigned < len(unassigned) {
		for _, gid := range gids {
			for count := 0; count < averageShardCount && assigned < len(unassigned); count, assigned = count+1, assigned+1 {
				config.Shards[unassigned[assigned]] = gid
			}
		}
		for _, gid := range gids {
			if assigned < len(unassigned) {
				config.Shards[unassigned[assigned]] = gid
				assigned++
			}
		}
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.done = make(map[int]chan int64)
	sc.lastRequest = make(map[int64]int64)

	sc.readPersistL(persister.ReadSnapshot())

	go sc.applier()

	return sc
}
