package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type state int

const (
	mapping state = iota
	reducing
	finished
)

type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex

	state state

	mapperId          int
	mapTasks          []string
	unmappedTask      map[string]bool
	mappingTask       map[string]int
	mappedTask        map[string]bool
	assignMapTaskTime map[string]time.Time

	reducerId            int
	reduceTasks          []int
	unreducedTask        map[int]bool
	reducingTask         map[int]int
	reducedTask          map[int]bool
	assignReduceTaskTime map[int]time.Time

	nReduce int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state == mapping {
		reply.Task = TaskMap
		c.mapperId++
		reply.MapperId = c.mapperId
		reply.NReduce = c.nReduce
		assignedTask := false
		for mapTask, isMapping := range c.unmappedTask {
			if !isMapping {
				reply.MapTaskFile = mapTask
				c.unmappedTask[mapTask] = true
				c.mappingTask[mapTask] = c.mapperId
				assignedTask = true
				c.assignMapTaskTime[mapTask] = time.Now()
				break
			}
		}
		if !assignedTask {
			for mapTask, isMapping := range c.unmappedTask {
				if isMapping && time.Since(c.assignMapTaskTime[mapTask]) > 10*time.Second {
					reply.MapTaskFile = mapTask
					c.unmappedTask[mapTask] = true
					c.mappingTask[mapTask] = c.mapperId
					assignedTask = true
					c.assignMapTaskTime[mapTask] = time.Now()
					break
				}
			}
		}
		if !assignedTask {
			reply.Task = TaskWait
		}
	} else if c.state == reducing {
		reply.Task = TaskReduce
		assignedTask := false
		for reduceTask, isReducing := range c.unreducedTask {
			if !isReducing {
				reply.ReducerId = reduceTask
				c.unreducedTask[reduceTask] = true
				c.reducingTask[reduceTask] = c.reducerId
				assignedTask = true
				c.assignReduceTaskTime[reduceTask] = time.Now()
				break
			}
		}
		if !assignedTask {
			for reduceTask, isReducing := range c.unreducedTask {
				if isReducing && time.Since(c.assignReduceTaskTime[reduceTask]) > 10*time.Second {
					reply.ReducerId = reduceTask
					c.unreducedTask[reduceTask] = true
					c.reducingTask[reduceTask] = c.reducerId
					assignedTask = true
					c.assignReduceTaskTime[reduceTask] = time.Now()
					break
				}
			}
		}
		reply.MapperId = c.mapperId
		if !assignedTask {
			reply.Task = TaskWait
		}
	} else if c.state == finished {
		reply.Task = TaskExit
	}

	return nil
}

func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state == mapping && args.Task == TaskMap {
		delete(c.unmappedTask, args.MapTaskFile)
		delete(c.mappingTask, args.MapTaskFile)
		delete(c.assignMapTaskTime, args.MapTaskFile)
		c.mappedTask[args.MapTaskFile] = true
		c.reduceTasks = append(c.reduceTasks, args.MapperId)
		c.unreducedTask[args.MapperId] = false
		if len(c.unmappedTask) == 0 && len(c.mappingTask) == 0 && len(c.mappedTask) == len(c.mapTasks) {
			c.state = reducing
		}
	} else if c.state == reducing && args.Task == TaskReduce {
		delete(c.unreducedTask, args.ReducerId)
		delete(c.reducingTask, args.ReducerId)
		delete(c.assignReduceTaskTime, args.ReducerId)
		c.reducedTask[args.ReducerId] = true
		for i := 0; i <= c.mapperId; i++ {
			os.Remove(fmt.Sprintf("mr-%d-%d", i, args.ReducerId))
		}
		if len(c.unreducedTask) == 0 && len(c.reducingTask) == 0 && len(c.reducedTask) == c.nReduce {
			c.state = finished
		}
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
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
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Your code here.
	ret := c.state == finished
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.state = mapping
	c.nReduce = nReduce
	c.mapTasks = append([]string(nil), files...)
	c.unmappedTask = make(map[string]bool)
	c.mappingTask = make(map[string]int)
	c.assignMapTaskTime = make(map[string]time.Time)
	c.mappedTask = make(map[string]bool)
	for _, mapTask := range c.mapTasks {
		c.unmappedTask[mapTask] = false
	}

	c.unreducedTask = make(map[int]bool)
	c.reducingTask = make(map[int]int)
	c.reducedTask = make(map[int]bool)
	c.assignReduceTaskTime = make(map[int]time.Time)
	for i := 0; i < nReduce; i++ {
		c.unreducedTask[i] = false
	}

	c.server()
	return &c
}
