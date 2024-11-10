package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Cli_ID     int64
	Command_ID uint32
}

type PutAppendReply struct {
	Error Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Cli_ID     int64
	Command_ID uint32
}

type GetReply struct {
	Error Err
	Value string
}
