package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	Files       []string
	Assignments map[string]int
	NReduce     int
	MapCount    int
	ReduceCount int
}

// Your code here -- RPC handlers for the worker to call.

type GetTaskArgs struct {
	WorkerID int
}

type GetTaskReply struct {
	PluginName      string
	InputFileName   string
	Operation       string
	OperationNumber int
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {

	// get a file to process
	for _, file := range c.Files {
		if _, ok := c.Assignments[file]; !ok {
			fmt.Printf("Found file %v\n", file)
			c.Assignments[file] = args.WorkerID
			reply.InputFileName = file
			reply.Operation = "map"
			c.MapCount++
			reply.OperationNumber = c.MapCount
			break
		}
	}
	return nil
}

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
func (c *Coordinator) Done() bool {
	ret := false

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Files:       files,
		NReduce:     nReduce,
		Assignments: make(map[string]int),
		MapCount:    0,
		ReduceCount: 0,
	}
	fmt.Printf("Coordinator: MakeCoordinator\n")
	fmt.Printf("Coordinator: files %v\n", files)

	// Your code here.
	c.server()
	return &c
}
