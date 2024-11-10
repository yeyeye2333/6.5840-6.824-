package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const max_command_time = time.Millisecond * 100

type rf_index = uint32
type rf_term = uint32

type oprate = int

const (
	get_type oprate = iota
	put_type
	append_type
)

type rpc_ret = int

const (
	complete rpc_ret = iota
	not_leader
	time_out
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type       oprate
	Key        string
	Val        string
	Cli_ID     int64
	Command_ID rf_index
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister  *raft.Persister
	kvs        map[string]string
	commit_map map[rf_index]chan rf_term
	cli_map    map[int64]rf_index
}

func (kv *KVServer) real_deal() {
	for !kv.killed() {
		apply := <-kv.applyCh
		kv.mu.Lock()
		if apply.CommandValid {
			op := apply.Command.(Op)
			if v, ok := kv.cli_map[op.Cli_ID]; !ok || v != op.Command_ID {
				switch op.Type {
				case get_type:
				case put_type:
					kv.kvs[op.Key] = op.Val
					kv.cli_map[op.Cli_ID] = op.Command_ID
				case append_type:
					if v, ok := kv.kvs[op.Key]; ok {
						kv.kvs[op.Key] = v + op.Val
					} else {
						kv.kvs[op.Key] = op.Val
					}
					kv.cli_map[op.Cli_ID] = op.Command_ID
				}
				if kv.maxraftstate > 0 && kv.maxraftstate < kv.persister.RaftStateSize() {
					w1 := new(bytes.Buffer)
					e1 := labgob.NewEncoder(w1)
					e1.Encode(kv.kvs)
					e1.Encode(kv.cli_map)
					snapshot := w1.Bytes()
					kv.rf.Snapshot(apply.CommandIndex, snapshot)
				}
			}
			if v, ok := kv.commit_map[uint32(apply.CommandIndex)]; ok {
				v <- apply.CommandTerm
			}
		} else if apply.SnapshotValid {
			r := bytes.NewBuffer(apply.Snapshot)
			d := labgob.NewDecoder(r)
			var kvs map[string]string
			var cli_map map[int64]uint32
			if err1 := d.Decode(&kvs); err1 != nil ||
				d.Decode(&cli_map) != nil {
				fmt.Println("快照读取失败", err1)
			} else {
				kv.cli_map = cli_map
				kv.kvs = kvs
			}
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) command_start(op Op) rpc_ret {
	kv.mu.Lock()
	if v, ok := kv.cli_map[op.Cli_ID]; ok {
		if v >= op.Command_ID && op.Type != get_type {
			kv.mu.Unlock()
			return complete
		}
	}
	kv.mu.Unlock()

	index, term, isleader := kv.rf.Start(op)
	if !isleader {
		return not_leader
	}

	ch := make(chan rf_term, 1)
	kv.mu.Lock() //可能晚于deal,无碍
	kv.commit_map[uint32(index)] = ch
	kv.mu.Unlock()

	select {
	case commit_term := <-ch:
		if commit_term == uint32(term) {
			kv.mu.Lock()
			delete(kv.commit_map, uint32(index)) //不考虑已有index（短时间连续成为leader）
			kv.mu.Unlock()
			return complete
		}
	case <-time.After(max_command_time):
	}
	kv.mu.Lock()
	delete(kv.commit_map, uint32(index))
	kv.mu.Unlock()
	return time_out
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		return
	}

	op := Op{Type: get_type, Key: args.Key, Cli_ID: args.Cli_ID, Command_ID: args.Command_ID}
	switch kv.command_start(op) {
	case complete:
		reply.Error = ""
		kv.mu.Lock()
		if v, ok := kv.kvs[args.Key]; ok {
			reply.Value = v
		}
		kv.mu.Unlock()
	case not_leader:
		reply.Error = "not leader"
	case time_out:
		reply.Error = "time out"
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		return
	}

	op := Op{Type: put_type, Key: args.Key, Val: args.Value, Cli_ID: args.Cli_ID, Command_ID: args.Command_ID}
	switch kv.command_start(op) {
	case complete:
		reply.Error = ""
	case not_leader:
		reply.Error = "not leader"
	case time_out:
		reply.Error = "time out"
	}
	// println("Err is", reply.Err)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		return
	}

	op := Op{Type: append_type, Key: args.Key, Val: args.Value, Cli_ID: args.Cli_ID, Command_ID: args.Command_ID}
	switch kv.command_start(op) {
	case complete:
		reply.Error = ""
	case not_leader:
		reply.Error = "not leader"
	case time_out:
		reply.Error = "time out"
	}
	// println("Err is", reply.Err)
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
	kv.kvs = make(map[string]string, 10)
	kv.commit_map = make(map[rf_index]chan rf_term, 10)
	kv.cli_map = make(map[int64]rf_index, 10)
	kv.persister = persister
	go kv.real_deal()

	return kv
}
