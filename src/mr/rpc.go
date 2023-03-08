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

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type Task int

const (
	TaskMap Task = iota
	TaskReduce
	TaskWait
	TaskExit
)

type AssignTaskArgs struct {
}

type AssignTaskReply struct {
	Task        Task
	MapperId    int
	NReduce     int
	MapTaskFile string
	ReducerId   int
}

type FinishTaskArgs struct {
	Task        Task
	MapperId    int
	MapTaskFile string
	ReducerId   int
}

type FinishTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
