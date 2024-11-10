package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)

type rf_ID = int16

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	leader_index rf_ID
	cli_ID       int64
	command_ID   uint32
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.leader_index = 0
	ck.cli_ID = nrand()
	ck.command_ID = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	// Your code here.
	for {
		// try each known server.
		args := &QueryArgs{Num: num, Cli_ID: ck.cli_ID, Command_ID: ck.command_ID}
		var reply QueryReply
		ok := ck.servers[ck.leader_index].Call("ShardCtrler.Query", args, &reply)
		if !ok || reply.WrongLeader {
			ck.leader_index++
			if ck.leader_index >= rf_ID(len(ck.servers)) {
				ck.leader_index = 0
			}
			continue
		}
		switch reply.Err {
		case OK:
			ck.command_ID++
			return reply.Config
		case ErrTimeOut:
		}
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	// Your code here.
	// fmt.Println("一次join")
	// for v := range servers {
	// fmt.Println("command", ck.command_ID)
	// }
	for {
		// try each known server.
		args := &JoinArgs{Servers: servers, Cli_ID: ck.cli_ID, Command_ID: ck.command_ID}
		var reply JoinReply
		ok := ck.servers[ck.leader_index].Call("ShardCtrler.Join", args, &reply)
		if !ok || reply.WrongLeader {
			ck.leader_index++
			if ck.leader_index >= rf_ID(len(ck.servers)) {
				ck.leader_index = 0
			}
			continue
		}
		switch reply.Err {
		case OK:
			ck.command_ID++
			return
		case ErrTimeOut:
		}
	}
}

func (ck *Clerk) Leave(gids []int) {
	// Your code here.

	for {
		// try each known server.
		args := &LeaveArgs{GIDs: gids, Cli_ID: ck.cli_ID, Command_ID: ck.command_ID}
		var reply LeaveReply
		ok := ck.servers[ck.leader_index].Call("ShardCtrler.Leave", args, &reply)
		if !ok || reply.WrongLeader {
			ck.leader_index++
			if ck.leader_index >= rf_ID(len(ck.servers)) {
				ck.leader_index = 0
			}
			continue
		}
		switch reply.Err {
		case OK:
			ck.command_ID++
			return
		case ErrTimeOut:
		}
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	// Your code here.

	for {
		// try each known server.
		args := &MoveArgs{Cli_ID: ck.cli_ID, Command_ID: ck.command_ID}
		args.Shard = shard
		args.GID = gid
		var reply MoveReply
		ok := ck.servers[ck.leader_index].Call("ShardCtrler.Move", args, &reply)
		if !ok || reply.WrongLeader {
			ck.leader_index++
			if ck.leader_index >= rf_ID(len(ck.servers)) {
				ck.leader_index = 0
			}
			continue
		}
		switch reply.Err {
		case OK:
			ck.command_ID++
			return
		case ErrTimeOut:
		}
	}
}
