package shardkv

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

const max_command_time = time.Millisecond * 100

type rf_index = uint32
type rf_term = uint32

type oprate = int

const (
	get_type oprate = iota
	put_type
	append_type

	config_type

	pull_shard
	push_shard
)

type rpc_ret = int

const (
	complete rpc_ret = iota
	not_leader
	err_group
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

	Config shardctrler.Config

	Shard_ID int
	Shards   map[string]string
	Cli_map  map[int64]rf_index
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	mck        *shardctrler.Clerk
	config     shardctrler.Config
	prevconfig shardctrler.Config
	persister  *raft.Persister
	kvs        map[string]string
	commit_map map[rf_index]chan rf_term
	cli_map    map[int64]rf_index

	shard_status map[int]bool             //没东西才能获取新配置 true为pull
	shard_num    [shardctrler.NShards]int //shard->已知最大config_num
}

func (kv *ShardKV) real_deal() {
	for {
		apply := <-kv.applyCh
		kv.mu.Lock()
		if apply.CommandValid {
			op := apply.Command.(Op)
			if _, ok := kv.shard_status[key2shard(op.Key)]; op.Type != config_type &&
				op.Type != pull_shard && op.Type != push_shard &&
				(kv.config.Shards[key2shard(op.Key)] != kv.gid || ok) {
				if v, ok := kv.commit_map[uint32(apply.CommandIndex)]; ok {
					v <- 0
				}
				kv.mu.Unlock()
				continue
			}
			if v, ok := kv.cli_map[op.Cli_ID]; !ok || v != op.Command_ID ||
				(op.Type == config_type || op.Type == pull_shard || op.Type == push_shard) {
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
				case config_type:
					if kv.config.Num < op.Config.Num {
						oldshard := make(map[int]bool)
						shard := make(map[int]bool)
						for k, v := range kv.config.Shards {
							if v == kv.gid {
								oldshard[k] = true
							}
						}
						for k, v := range op.Config.Shards {
							if v == kv.gid {
								shard[k] = true
							}
						}
						// for i := range shard {
						// 	kv.shard_num[i] = op.Config.Num
						// }
						// for k := range oldshard {
						// 	if _, ok := shard[k]; !ok {
						// 		kv.shard_status[k] = false
						// 	}
						// }
						if op.Config.Num > 1 {
							for k := range shard {
								if _, ok := oldshard[k]; !ok {
									kv.shard_status[k] = true
								}
							}
						}

						kv.prevconfig = kv.config
						kv.config = op.Config
					}
				case pull_shard:
					if _, ok := kv.shard_status[op.Shard_ID]; ok {
						for k, v := range op.Shards {
							kv.kvs[k] = v
						}
						for k, v := range op.Cli_map {
							if v2, ok := kv.cli_map[k]; !ok || v2 < v {
								kv.cli_map[k] = v
							}
						}
						kv.shard_num[op.Shard_ID] = kv.config.Num
						delete(kv.shard_status, op.Shard_ID)
					}
				case push_shard:
				}
				if kv.maxraftstate > 0 && kv.maxraftstate < kv.persister.RaftStateSize() {
					w1 := new(bytes.Buffer)
					e1 := labgob.NewEncoder(w1)
					e1.Encode(kv.kvs)
					e1.Encode(kv.cli_map)
					e1.Encode(kv.config)
					e1.Encode(kv.prevconfig)
					e1.Encode(kv.shard_status)
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
			var config shardctrler.Config
			var prevconfig shardctrler.Config
			var shard_status map[int]bool
			err1 := d.Decode(&kvs)
			err2 := d.Decode(&cli_map)
			err3 := d.Decode(&config)
			err4 := d.Decode(&prevconfig)
			err5 := d.Decode(&shard_status)
			if err1 != nil || err2 != nil || err3 != nil || err4 != nil || err5 != nil {
				fmt.Println("快照读取失败", err1, err2, err3, err4, err5)
			} else {
				kv.cli_map = cli_map
				kv.kvs = kvs
				kv.config = config
				kv.prevconfig = prevconfig
				kv.shard_status = shard_status //
			}
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) command_start(op Op) rpc_ret {
	if op.Type != config_type && op.Type != pull_shard && op.Type != push_shard {
		kv.mu.Lock()
		if kv.config.Shards[key2shard(op.Key)] != kv.gid {
			kv.mu.Unlock()
			return err_group
		}
		if _, ok := kv.shard_status[key2shard(op.Key)]; ok {
			kv.mu.Unlock()
			return time_out
		}
		if v, ok := kv.cli_map[op.Cli_ID]; ok {
			if v >= op.Command_ID && op.Type != get_type {
				kv.mu.Unlock()
				return complete
			}
		}
		kv.mu.Unlock()
	}

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
			// if op.Type == get_type {
			// 	fmt.Println(kv.gid, "get", key2shard(op.Key), " complete")
			// }
			return complete
		}
	case <-time.After(max_command_time):
	}
	kv.mu.Lock()
	delete(kv.commit_map, uint32(index))
	kv.mu.Unlock()
	return time_out
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{Type: get_type, Key: args.Key, Cli_ID: args.Cli_ID, Command_ID: args.Command_ID}
	if args.Config_Num > (kv.config.Num + 1) {
		reply.Err = ErrWrongGroup
		return
	}
	// fmt.Println(args.Config_Num, kv.gid, kv.config.Num, "尝试get", key2shard(args.Key))
	switch kv.command_start(op) {
	case complete:
		reply.Err = OK
		kv.mu.Lock()
		if v, ok := kv.kvs[args.Key]; ok {
			reply.Value = v
			// fmt.Println(args.Config_Num, kv.gid, kv.config.Num, "get", key2shard(args.Key), "是", reply.Value)
		} else {
			reply.Err = ErrNoKey
			// fmt.Println(args.Config_Num, kv.gid, kv.config.Num, "get", key2shard(args.Key), "时", kv.shard_status, kv.config, kv.prevconfig)
			// fmt.Println(kv.gid, kv.kvs)
		}
		kv.mu.Unlock()
	case err_group:
		reply.Err = ErrWrongGroup
	case not_leader:
		reply.Err = ErrWrongLeader
	case time_out:
		reply.Err = ErrTimeOut
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{Key: args.Key, Val: args.Value, Cli_ID: args.Cli_ID, Command_ID: args.Command_ID}
	if args.Config_Num > (kv.config.Num + 1) {
		reply.Err = ErrWrongGroup
		return
	}
	if args.Op == "Put" {
		op.Type = put_type
	} else if args.Op == "Append" {
		op.Type = append_type
	}
	switch kv.command_start(op) {
	case complete:
		// fmt.Println(args.Config_Num, "putappend", key2shard(args.Key), "的", args.Key, args.Value, "成功")
		reply.Err = OK
	case err_group:
		reply.Err = ErrWrongGroup
	case not_leader:
		reply.Err = ErrWrongLeader
	case time_out:
		reply.Err = ErrTimeOut
	}
}

type ShardArgs struct {
	Shard_ID int
	Num      int
}
type ShardReply struct {
	Err     Err
	Shards  map[string]string
	Cli_map map[int64]rf_index
}

func (kv *ShardKV) pull_shard(shard int, config shardctrler.Config, finish *bool) {
	args := ShardArgs{}
	args.Shard_ID = shard
	args.Num = config.Num

	gid := config.Shards[shard]
	if servers, ok := config.Groups[gid]; ok {
		// try each server for the shard.
		for si := 0; si <= len(servers); si++ {
			// fmt.Println(kv.gid, "向", gid, "拉取", shard, "在", config.Num)
			if si == len(servers) {
				si = 0
			}
			srv := kv.make_end(servers[si])
			var reply ShardReply
			ok := srv.Call("ShardKV.Shard_migration", &args, &reply)
			if !ok {
				continue
			}
			switch reply.Err {
			case OK:
				op := Op{Type: pull_shard, Shard_ID: shard, Shards: reply.Shards, Cli_map: reply.Cli_map}
				if kv.command_start(op) != complete {
					*finish = true
				} else {
					// fmt.Println(kv.gid, "向", gid, "拉取", shard, "成功")
					// fmt.Println(kv.gid, kv.kvs)
				}
				return
			case ErrWrongGroup:
				*finish = true
				return
			case ErrWrongLeader:
				continue
			case ErrTimeOut:
				// fmt.Println(kv.gid, "向", gid, "拉取", shard, "ErrTimeOut")
				si--
				continue
			}
		}
	}
}
func (kv *ShardKV) Shard_migration(args *ShardArgs, reply *ShardReply) {
	op := Op{Type: push_shard, Shard_ID: args.Shard_ID}
	// fmt.Println(kv.gid, "收到拉取请求", args.Shard_ID)

	if args.Num < kv.shard_num[args.Shard_ID] { //去了过得更快，6
		// fmt.Println(kv.gid, "发送", args.Shard_ID, "空配置")
		reply.Err = OK
		reply.Shards = make(map[string]string)
		reply.Cli_map = make(map[int64]uint32)
		return
	}
	switch kv.command_start(op) {
	case complete:
		if kv.config.Num <= args.Num { //本地配置需大于对方老配置
			reply.Err = ErrWrongGroup
			// fmt.Println(kv.gid, "未配置完", kv.shard_status, kv.config.Num, "<", args.Num)
			// fmt.Println(kv.gid, kv.config, kv.shard_status, kv.prevconfig)
			return
		}

		reply.Err = OK
		shards := make(map[string]string)
		kv.mu.Lock()
		for k, v := range kv.kvs {
			if key2shard(k) == args.Shard_ID {
				shards[k] = v
			}
		}
		reply.Cli_map = kv.cli_map
		kv.mu.Unlock()
		reply.Shards = shards
		// fmt.Println(kv.gid, "push了", shards)
	case not_leader:
		reply.Err = ErrWrongLeader
	case time_out:
		reply.Err = ErrTimeOut
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	fmt.Println(kv.gid, kv.me)
	kv.rf.Kill()
	// Your code here, if desired.
}

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
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(shardctrler.Config{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.kvs = make(map[string]string, 10)
	kv.commit_map = make(map[rf_index]chan rf_term, 10)
	kv.cli_map = make(map[int64]rf_index, 10)
	kv.persister = persister

	kv.config = shardctrler.Config{Num: 0, Groups: map[int][]string{}}
	kv.prevconfig = shardctrler.Config{Num: 0, Groups: map[int][]string{}}

	kv.shard_status = make(map[int]bool)

	go func() {
		var finishs [shardctrler.NShards]bool
		for i := range finishs {
			finishs[i] = true
		}
		for {
			_, isleader := kv.rf.GetState()
			kv.mu.Lock()
			if isleader && len(kv.shard_status) == 0 {
				kv.mu.Unlock()
				config := kv.mck.Query(kv.config.Num + 1)
				if config.Num > kv.config.Num {
					// fmt.Println(kv.gid, "新配置", config, kv.config)
					op := Op{Type: config_type, Config: config}
					if kv.command_start(op) == complete {
						for i := range finishs {
							finishs[i] = true
						}
					}
				}
			} else if isleader && len(kv.shard_status) != 0 {
				for i := range kv.shard_status {
					if finishs[i] {
						finishs[i] = false
						go kv.pull_shard(i, kv.prevconfig, &finishs[i])
					}
				}
				kv.mu.Unlock()
			} else {
				kv.mu.Unlock()
			}
			time.Sleep(time.Millisecond * 100)
		}
	}()
	go kv.real_deal()

	return kv
}
