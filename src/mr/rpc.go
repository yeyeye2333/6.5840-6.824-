package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// type ExampleArgs struct {
// 	X int
// }

// type ExampleReply struct {
// 	Y int
// }

// Add your RPC definitions here.
type task_type int32

const (
	t_map task_type = iota
	t_reduce
	nothing
)

type Find_args struct {
}
type Find_reply struct {
	File       string
	ID         int
	Task       task_type
	Reduce_num int
}

type Finish_args struct {
	ID   int
	Task task_type
}
type Finish_reply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
