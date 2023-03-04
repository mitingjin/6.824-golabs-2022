package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	Op        string
	ClientId  int64
	RequestId int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db          map[string]string
	done        map[int]chan int64
	lastRequest map[int64]int64

	lastApplied int
}

func (kv *KVServer) persistentStateL() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.db)
	e.Encode(kv.lastRequest)
	return w.Bytes()
}

func (kv *KVServer) readPersistL(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var db map[string]string
	var lastRequest map[int64]int64
	if d.Decode(&db) != nil || d.Decode(&lastRequest) != nil {

	} else {
		kv.db = db
		kv.lastRequest = lastRequest
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := Op{
		Key:       args.Key,
		Op:        "Get",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	if ok := kv.waitForAppliedL(op); ok {
		if v, exists := kv.db[op.Key]; exists {
			reply.Value = v
			reply.Err = OK
		} else {
			reply.Err = ErrNoKey
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := Op{
		Key:       args.Key,
		Value:     args.Value,
		Op:        args.Op,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	if ok := kv.waitForAppliedL(op); ok {
		reply.Err = OK
	}
}

func (kv *KVServer) waitForAppliedL(op Op) bool {
	if _, isLeader := kv.rf.GetState(); isLeader {
		index, _, _ := kv.rf.Start(op)
		if _, ok := kv.done[index]; !ok {
			kv.done[index] = make(chan int64)
		}
		ch := kv.done[index]
		kv.mu.Unlock()
		defer kv.mu.Lock()
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

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applier() {
	for kv.killed() == false {
		for msg := range kv.applyCh {
			kv.mu.Lock()
			if msg.CommandValid {
				rid := kv.applyL(msg.Command.(Op))
				if msg.CommandIndex > kv.lastApplied {
					kv.lastApplied = msg.CommandIndex
				}
				if _, isLeader := kv.rf.GetState(); isLeader {
					ch := kv.done[msg.CommandIndex]
					kv.mu.Unlock()
					select {
					case ch <- rid:
					default:
					}
					kv.mu.Lock()
				}
				if kv.maxraftstate != -1 && kv.rf.RaftStateSize() >= kv.maxraftstate {
					kv.rf.Snapshot(kv.lastApplied, kv.persistentStateL())
				}
			} else {
				kv.readPersistL(msg.Snapshot)
				kv.lastApplied = msg.SnapshotIndex
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) applyL(op Op) (rid int64) {
	rid = op.RequestId
	if kv.lastRequest[op.ClientId] < op.RequestId {
		switch op.Op {
		case "Get":
			return
		case "Put":
			kv.db[op.Key] = op.Value
		case "Append":
			kv.db[op.Key] += op.Value
		}
		kv.lastRequest[op.ClientId] = op.RequestId
	}
	return
}

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

	kv.db = make(map[string]string)
	kv.done = make(map[int]chan int64)
	kv.lastRequest = make(map[int64]int64)

	kv.readPersistL(persister.ReadSnapshot())

	go kv.applier()

	return kv
}
