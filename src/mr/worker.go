package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		args := AssignTaskArgs{}
		reply := AssignTaskReply{}
		if ok := call("Coordinator.AssignTask", &args, &reply); ok {
			switch reply.Task {
			case TaskMap:
				filename := reply.MapTaskFile
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				content, err := io.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}
				file.Close()
				kva := mapf(filename, string(content))
				tempFiles := make([]*os.File, reply.NReduce)
				for i := 0; i < reply.NReduce; i++ {
					tempFiles[i], err = os.CreateTemp(".", "tmp")
				}
				encoders := make([]*json.Encoder, reply.NReduce)
				for i := 0; i < reply.NReduce; i++ {
					encoders[i] = json.NewEncoder(tempFiles[i])
				}
				var reducerId int
				for _, kv := range kva {
					reducerId = ihash(kv.Key) % reply.NReduce
					encoders[reducerId].Encode(&kv)
				}
				for i := 0; i < reply.NReduce; i++ {
					os.Rename(tempFiles[i].Name(), fmt.Sprintf("mr-%d-%d", reply.MapperId, i))
				}
				finishTaskArgs := FinishTaskArgs{
					Task:        TaskMap,
					MapperId:    reply.MapperId,
					MapTaskFile: reply.MapTaskFile,
				}
				finishTaskReply := FinishTaskReply{}
				CallFinishTask(&finishTaskArgs, &finishTaskReply)
			case TaskReduce:
				var intermediate []KeyValue
				mapperFileCount := reply.MapperId + 1
				reduceFiles := make([]*os.File, mapperFileCount)
				for i := 0; i < len(reduceFiles); i++ {
					reduceFiles[i], _ = os.Open(fmt.Sprintf("mr-%d-%d", i, reply.ReducerId))
				}
				decoders := make([]*json.Decoder, mapperFileCount)
				for i := 0; i < len(decoders); i++ {
					decoders[i] = json.NewDecoder(reduceFiles[i])
				}
				for i := 0; i < mapperFileCount; i++ {
					for {
						var kv KeyValue
						if err := decoders[i].Decode(&kv); err != nil {
							break
						}
						intermediate = append(intermediate, kv)
					}
				}
				for i := 0; i < len(reduceFiles); i++ {
					reduceFiles[i].Close()
				}
				sort.Sort(ByKey(intermediate))
				oname := fmt.Sprintf("mr-out-%d", reply.ReducerId)
				ofile, _ := os.Create(oname)
				i := 0
				for i < len(intermediate) {
					j := i + 1
					for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, intermediate[k].Value)
					}
					output := reducef(intermediate[i].Key, values)
					fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
					i = j
				}
				ofile.Close()
				finishTaskArgs := FinishTaskArgs{
					Task:      TaskReduce,
					ReducerId: reply.ReducerId,
				}
				finishTaskReply := FinishTaskReply{}
				CallFinishTask(&finishTaskArgs, &finishTaskReply)
			case TaskWait:

			case TaskExit:
				return
			}
		} else {
			return
		}
		time.Sleep(time.Second)
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func CallFinishTask(args *FinishTaskArgs, reply *FinishTaskReply) bool {
	ok := call("Coordinator.FinishTask", &args, &reply)
	return ok
}

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
