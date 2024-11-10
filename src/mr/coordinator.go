package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type task struct {
	string
	id       int
	isfinish bool
}
type Coordinator struct {
	// Your definitions here.
	maps       []task
	reduces    []task
	reduce_num int
	waitings   []*task
	beings     []*task
	mtx        sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Find_work(argvs Find_args, reply *Find_reply) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	reply.Reduce_num = c.reduce_num
	if len(c.waitings) == 0 && len(c.beings) == 0 {
		if len(c.reduces) == 0 {
			for i := 0; i < c.reduce_num; i++ {
				c.reduces = append(c.reduces, task{id: i, isfinish: false})
				c.waitings = append(c.waitings, &c.reduces[i])
			}
			reply.Task = nothing
		} else {
			done = true
			reply.Task = nothing
		}
	} else if len(c.waitings) > 0 {
		c.beings = append(c.beings, c.waitings[0])
		if len(c.reduces) == 0 {
			reply.File = c.waitings[0].string
			reply.Task = t_map
		} else {
			reply.Task = t_reduce
		}
		go func(t *task) {
			time.Sleep(time.Second * 10)
			c.mtx.Lock()
			defer c.mtx.Unlock()
			if !t.isfinish {
				c.waitings = append(c.waitings, t)
				for i := range c.beings {
					if c.beings[i].id == t.id {
						// fmt.Println("超时")
						tmp := c.beings[:i]
						c.beings = append(tmp, c.beings[i+1:len(c.beings)]...)
						break
					}
				}
			}
		}(c.waitings[0])
		reply.ID = c.waitings[0].id
		c.waitings = c.waitings[1:] //删头，内存泄漏,不好
	} else {
		reply.Task = nothing
	}
	return nil
}

func (c *Coordinator) Finish_ok(argvs Finish_args, reply *Finish_reply) error {
	// fmt.Println(argvs.Task, argvs.ID)
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if len(c.reduces) > 0 && argvs.Task == t_map {
		return errors.New("重复任务")
	}
	for i, v := range c.beings {
		if v.id == argvs.ID {
			v.isfinish = true
			tmp := c.beings[:i]
			c.beings = append(tmp, c.beings[i+1:len(c.beings)]...)
			return nil
		}
	}
	for i, v := range c.waitings {
		if v.id == argvs.ID {
			v.isfinish = true
			tmp := c.waitings[:i]
			c.waitings = append(tmp, c.waitings[i+1:len(c.waitings)]...)
			return nil
		}
	}
	return errors.New("重复任务")
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
// func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
var done bool

func (c *Coordinator) Done() bool {
	// Your code here.
	c.mtx.Lock()
	defer c.mtx.Unlock()
	return done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{reduce_num: nReduce}

	// Your code here.
	done = false
	id := 0
	for i := range files {
		c.maps = append(c.maps, task{string: files[i], id: id, isfinish: false})
		c.waitings = append(c.waitings, &c.maps[i])
		id++
	}

	c.server()
	return &c
}
