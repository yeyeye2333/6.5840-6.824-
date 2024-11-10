package shardctrler

import (
	"sort"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const max_command_time = time.Millisecond * 100

type rf_index = uint32
type rf_term = uint32

type oprate = int

const (
	join_type oprate = iota
	leave_type
	move_type
	query_type
)

type rpc_ret = int

const (
	complete rpc_ret = iota
	not_leader
	time_out
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	commit_map map[rf_index]chan rf_term
	cli_map    map[int64]rf_index //gid commandindex
	gid_shard  map[int][]int
	gid_groups map[int][]string

	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.
	Type oprate

	Servers map[int][]string

	GIDs []int

	GID   int
	Shard int

	Config_ID int

	Cli_ID     int64
	Command_ID rf_index
}

// 最大的最大的
func (sc *ShardCtrler) maxgroup() int {
	max := 0
	maxgid := 0
	for i := range sc.gid_shard {
		if max < len(sc.gid_shard[i]) || (max == len(sc.gid_shard[i]) && maxgid < i) {
			max = len(sc.gid_shard[i])
			maxgid = i
		}
	}
	return maxgid
}

// 最小的最大的
func (sc *ShardCtrler) mingroup() int {
	min := NShards + 1
	mingid := 0
	for i := range sc.gid_shard {
		if min > len(sc.gid_shard[i]) || (min == len(sc.gid_shard[i]) && mingid < i) {
			min = len(sc.gid_shard[i])
			mingid = i
		}
	}
	return mingid
}

func (sc *ShardCtrler) shard_sort() {
	for k := range sc.gid_shard {
		sort.Ints(sc.gid_shard[k])
	}
}

// 内部无锁
func (sc *ShardCtrler) rebalance() {
	changed := true
	if _, ok := sc.gid_shard[0]; len(sc.gid_shard) > 1 && ok {
		delete(sc.gid_shard, 0)
	}
	sc.shard_sort()
	for {
		max := sc.maxgroup()
		min := sc.mingroup()
		if len(sc.gid_shard[max])-len(sc.gid_shard[min]) <= 1 {
			break
		}
		changed = true
		sc.gid_shard[min] = append(sc.gid_shard[min], sc.gid_shard[max][len(sc.gid_shard[max])-1])
		sc.gid_shard[max] = sc.gid_shard[max][:len(sc.gid_shard[max])-1]
		sort.Ints(sc.gid_shard[min])
	}
	var shards [NShards]int
	for k := range sc.gid_shard {
		for i := range sc.gid_shard[k] {
			shards[sc.gid_shard[k][i]] = k
		}
	}
	for i := range shards {
		if shards[i] == 0 {
			changed = true
			min := sc.mingroup()
			sc.gid_shard[min] = append(sc.gid_shard[min], i)
			shards[i] = min
		}
	}
	if changed {
		groups := make(map[int][]string)
		for k := range sc.gid_groups {
			groups[k] = make([]string, 0)
			groups[k] = append(groups[k], sc.gid_groups[k]...)
		}
		sc.configs = append(sc.configs,
			Config{Num: sc.configs[len(sc.configs)-1].Num + 1, Shards: shards, Groups: groups})
	}
}

func (sc *ShardCtrler) real_deal() {
	for {
		apply := <-sc.applyCh
		sc.mu.Lock()
		if apply.CommandValid {
			op := apply.Command.(Op)
			if v, ok := sc.cli_map[op.Cli_ID]; !ok || v != op.Command_ID {
				switch op.Type {
				case query_type:
				case join_type:
					for gid, server := range op.Servers {
						if _, ok := sc.gid_shard[gid]; !ok {
							sc.gid_shard[gid] = []int{}
						}
						sc.gid_groups[gid] = server
					}
					sc.rebalance()
					sc.cli_map[op.Cli_ID] = op.Command_ID
					// fmt.Println(sc.me, "join", sc.configs[len(sc.configs)-1]) //
				case leave_type:
					for _, gid := range op.GIDs {
						delete(sc.gid_shard, gid)
						delete(sc.gid_groups, gid)
					}
					sc.rebalance()
					sc.cli_map[op.Cli_ID] = op.Command_ID
					// fmt.Println(sc.me, "leave", sc.configs[len(sc.configs)-1]) //
				case move_type:
					src := sc.configs[len(sc.configs)-1].Shards[op.Shard]
					for i, v := range sc.gid_shard[src] {
						if v == op.Shard {
							sc.gid_shard[op.GID] = append(sc.gid_shard[op.GID], v)
							if i+1 < len(sc.gid_shard[src]) {
								sc.gid_shard[src] = append(sc.gid_shard[src][:i], sc.gid_shard[src][i+1:]...)
							} else {
								sc.gid_shard[src] = sc.gid_shard[src][:i]
							}
							groups := make(map[int][]string)
							for k := range sc.gid_groups {
								groups[k] = make([]string, 0)
								groups[k] = append(groups[k], sc.gid_groups[k]...)
							}
							sc.configs = append(sc.configs,
								Config{Num: sc.configs[len(sc.configs)-1].Num + 1,
									Shards: sc.configs[len(sc.configs)-1].Shards, Groups: groups})
							sc.configs[len(sc.configs)-1].Shards[op.Shard] = op.GID
							break
						}
					}
				}
			}
			if v, ok := sc.commit_map[uint32(apply.CommandIndex)]; ok {
				v <- apply.CommandTerm
			}
		}
		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) command_start(op Op) rpc_ret {
	sc.mu.Lock()
	if v, ok := sc.cli_map[op.Cli_ID]; ok {
		if v >= op.Command_ID && op.Type != query_type {
			sc.mu.Unlock()
			return complete
		}
	}
	sc.mu.Unlock()

	index, term, isleader := sc.rf.Start(op)
	if !isleader {
		return not_leader
	}

	ch := make(chan rf_term, 1)
	sc.mu.Lock() //可能晚于deal,无碍
	sc.commit_map[uint32(index)] = ch
	sc.mu.Unlock()

	select {
	case commit_term := <-ch:
		if commit_term == uint32(term) {
			sc.mu.Lock()
			delete(sc.commit_map, uint32(index)) //不考虑已有index（短时间连续成为leader）
			sc.mu.Unlock()
			return complete
		}
	case <-time.After(max_command_time):
	}
	sc.mu.Lock()
	delete(sc.commit_map, uint32(index))
	sc.mu.Unlock()
	return time_out
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{Type: join_type, Servers: args.Servers, Cli_ID: args.Cli_ID, Command_ID: args.Command_ID}
	switch sc.command_start(op) {
	case complete:
		reply.Err = OK
		reply.WrongLeader = false
	case time_out:
		reply.Err = ErrTimeOut
		reply.WrongLeader = false
	case not_leader:
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{Type: leave_type, GIDs: args.GIDs, Cli_ID: args.Cli_ID, Command_ID: args.Command_ID}
	switch sc.command_start(op) {
	case complete:
		reply.Err = OK
		reply.WrongLeader = false
	case time_out:
		reply.Err = ErrTimeOut
		reply.WrongLeader = false
	case not_leader:
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	// fmt.Println("move", args.GID, args.Shard)
	op := Op{Type: move_type, GID: args.GID, Shard: args.Shard, Cli_ID: args.Cli_ID, Command_ID: args.Command_ID}
	switch sc.command_start(op) {
	case complete:
		reply.Err = OK
		reply.WrongLeader = false
	case time_out:
		reply.Err = ErrTimeOut
		reply.WrongLeader = false
	case not_leader:
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{Type: query_type, Config_ID: args.Num, Cli_ID: args.Cli_ID, Command_ID: args.Command_ID}
	switch sc.command_start(op) {
	case complete:
		reply.Err = OK
		reply.WrongLeader = false
		sc.mu.Lock()
		if args.Num < 0 || args.Num > len(sc.configs)-1 {
			reply.Config = sc.configs[len(sc.configs)-1]
		} else {
			reply.Config = sc.configs[args.Num]
		}
		sc.mu.Unlock()
	case time_out:
		reply.Err = ErrTimeOut
		reply.WrongLeader = false
	case not_leader:
		reply.WrongLeader = true
	}
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
	sc.commit_map = make(map[uint32]chan uint32)
	sc.cli_map = make(map[int64]uint32)
	sc.gid_groups = make(map[int][]string)
	sc.gid_shard = make(map[int][]int)
	go sc.real_deal()

	return sc
}
