package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
func maptask(mapf func(string, string) []KeyValue, reply Find_reply) {
	// fmt.Println("maptask")
	filename := reply.File
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	intermediate := mapf(filename, string(content))

	HashKv := make([][]KeyValue, reply.Reduce_num)
	for _, kv := range intermediate {
		HashKv[ihash(kv.Key)%reply.Reduce_num] = append(HashKv[ihash(kv.Key)%reply.Reduce_num], kv)
	}
	for i := 0; i < reply.Reduce_num; i++ {
		ofile, err := os.CreateTemp("", "")
		if err != nil {
			return
		}
		defer func(f *os.File) {
			f.Close()
		}(ofile)
		for _, kv := range HashKv[i] {
			fmt.Fprintf(ofile, "%v %v\n", kv.Key, kv.Value)
		}
		oldname := ofile.Name()
		cmd := exec.Command("mv", oldname, "mr-"+strconv.Itoa(reply.ID)+"-"+strconv.Itoa(i))
		_, err = cmd.Output()
		if err != nil {
			fmt.Println(err)
			return
		}
	}
	fin_arg := Finish_args{ID: reply.ID, Task: t_map}
	call("Coordinator.Finish_ok", fin_arg, new(Finish_reply))
}
func reducetask(reducef func(string, []string) string, reply Find_reply) {
	// fmt.Println("reducetask")
	ofile, _ := os.CreateTemp("", "")
	defer ofile.Close()
	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	files, err := ioutil.ReadDir("./")
	if err != nil {
		return
	}
	var ifile *os.File
	var context []byte
	for _, file := range files {
		if strings.HasSuffix(file.Name(), strconv.Itoa(reply.ID)) {
			ifile, err = os.Open(file.Name())
			if err != nil {
				return
			}
			defer func(f *os.File) {
				f.Close()
			}(ifile)
			tmp, _ := ioutil.ReadAll(ifile)
			context = append(context, tmp...)
		}
	}
	intermediate := []KeyValue{}
	strContent := string(context)
	strSlice := strings.Split(strContent, "\n")
	for _, row := range strSlice {
		kvSlice := strings.Split(row, " ")
		if len(kvSlice) == 2 {
			intermediate = append(intermediate, KeyValue{Key: kvSlice[0], Value: kvSlice[1]})
		}
	}
	sort.Sort(ByKey(intermediate))

	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	cmd := exec.Command("mv", ofile.Name(), "mr-out-"+strconv.Itoa(reply.ID))
	_, err = cmd.Output()
	if err != nil {
		fmt.Println(err)
		return
	}
	fin_arg := Finish_args{ID: reply.ID, Task: t_reduce}
	call("Coordinator.Finish_ok", fin_arg, new(Finish_reply))
}
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	var arg Find_args
	for {
		reply := Find_reply{}
		if !call("Coordinator.Find_work", arg, &reply) {
			return
		}
		if reply.Task == t_map {
			maptask(mapf, reply)
		} else if reply.Task == t_reduce {
			reducetask(reducef, reply)
		} else if reply.Task == nothing {
			time.Sleep(time.Second * 5)
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
// func CallExample() {

// declare an argument structure.
// args := ExampleArgs{}

// fill in the argument(s).
// args.X = 99

// declare a reply structure.
// reply := ExampleReply{}

// send the RPC request, wait for the reply.
// the "Coordinator.Example" tells the
// receiving server that we'd like to call
// the Example() method of struct Coordinator.
// 	ok := call("Coordinator.Example", &args, &reply)
// 	if ok {
// 		// reply.Y should be 100.
// 		fmt.Printf("reply.Y %v\n", reply.Y)
// 	} else {
// 		fmt.Printf("call failed!\n")
// 	}
// }

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
