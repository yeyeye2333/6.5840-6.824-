package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)

type rf_ID = int16

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
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
	// You'll have to add code here.
	ck.leader_index = 0
	ck.cli_ID = nrand()
	ck.command_ID = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	// fmt.Println("发送get")
	for {
		args := GetArgs{Key: key, Cli_ID: ck.cli_ID, Command_ID: ck.command_ID}
		var reply GetReply
		ok := ck.servers[ck.leader_index].Call("KVServer.Get", &args, &reply)
		if !ok {
			ck.leader_index++
			if ck.leader_index >= rf_ID(len(ck.servers)) {
				ck.leader_index = 0
			}
			continue
		}
		switch reply.Error {
		case "":
			ck.command_ID++
			return reply.Value
		case "not leader":
			ck.leader_index++
			if ck.leader_index >= rf_ID(len(ck.servers)) {
				ck.leader_index = 0
			}
		case "time out":
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	// fmt.Println("发送 put/app")
	for {
		args := PutAppendArgs{Key: key, Value: value, Cli_ID: ck.cli_ID, Command_ID: ck.command_ID}
		var reply PutAppendReply
		ok := ck.servers[ck.leader_index].Call("KVServer."+op, &args, &reply)
		// fmt.Println(reply.Error, i, len(ck.servers))
		if !ok {
			ck.leader_index++
			if ck.leader_index >= rf_ID(len(ck.servers)) {
				ck.leader_index = 0
			}
			continue
		}
		switch reply.Error {
		case "":
			ck.command_ID++
			return
		case "time out":
		case "not leader":
			ck.leader_index++
			if ck.leader_index >= rf_ID(len(ck.servers)) {
				ck.leader_index = 0
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
