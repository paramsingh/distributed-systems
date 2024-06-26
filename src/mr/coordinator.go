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

type Task struct {
	workerID   int
	completed  bool
	assigned   bool
	assignedAt time.Time
}

type Coordinator struct {
	mu          sync.Mutex
	Files       []string
	NReduce     int
	NMap        int
	MapTasks    []Task
	ReduceTasks []Task
	Phase       string
	done        chan struct{}
}

type GetTaskArgs struct {
	WorkerID int
}

type GetTaskReply struct {
	InputFileName   string
	Operation       string
	OperationNumber int
	NMap            int
	NReduce         int
	WaitForTask     bool
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	found := false
	for i, task := range c.MapTasks {
		if !task.completed && !task.assigned {
			found = true
			reply.InputFileName = c.Files[i]
			reply.Operation = "map"
			reply.OperationNumber = i
			reply.NMap = c.NMap
			reply.NReduce = c.NReduce
			reply.WaitForTask = false
			c.MapTasks[i].workerID = args.WorkerID
			c.MapTasks[i].assigned = true
			c.MapTasks[i].assignedAt = time.Now()
			fmt.Printf("coordinator: map tasks %v\n", c.MapTasks)
			fmt.Printf("coordinator: reduce tasks %v\n", c.ReduceTasks)
			break
		}
	}

	if found {
		fmt.Printf("coordinator: found map task: returning\n")
		return nil
	}

	if !c.AllMapTasksCompleted() {
		reply.WaitForTask = true
		fmt.Print("no tasks available, wait\n")
		return nil
	}

	fmt.Printf("coordinator: no map tasks available, looking for reduce tasks\n")

	// get a reduce task
	for i, task := range c.ReduceTasks {
		if !task.completed && !task.assigned {
			reply.Operation = "reduce"
			reply.OperationNumber = i
			reply.NReduce = c.NReduce
			reply.NMap = c.NMap
			c.ReduceTasks[i].workerID = args.WorkerID
			c.ReduceTasks[i].assigned = true
			c.ReduceTasks[i].assignedAt = time.Now()
			reply.WaitForTask = false
			fmt.Printf("coordinator: map tasks %v\n", c.MapTasks)
			fmt.Printf("coordinator: reduce tasks %v\n", c.ReduceTasks)
			return nil
		}
	}

	if c.AllReduceTasksCompleted() {
		return fmt.Errorf("all tasks completed")
	}
	fmt.Printf("coordinator: no reduce tasks available, wait\n")
	reply.WaitForTask = true
	return nil
}

type MarkTaskCompletedArgs struct {
	Operation       string
	OperationNumber int
}

type MarkTaskCompletedReply struct{}

func (c *Coordinator) MarkTaskCompleted(args *MarkTaskCompletedArgs, reply *MarkTaskCompletedReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.Operation == "map" {
		c.MapTasks[args.OperationNumber].completed = true
		return nil
	} else if args.Operation == "reduce" {
		c.ReduceTasks[args.OperationNumber].completed = true
		return nil
	}
	return fmt.Errorf("invalid operation")
}

func (c *Coordinator) AllMapTasksCompleted() bool {
	for _, task := range c.MapTasks {
		if !task.completed {
			return false
		}
	}
	return true
}

func (c *Coordinator) AllReduceTasksCompleted() bool {
	for _, task := range c.ReduceTasks {
		if !task.completed {
			return false
		}
	}
	return true
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
	if c.AllMapTasksCompleted() && c.AllReduceTasksCompleted() {
		close(c.done)
		return true
	}

	return false
}

func (c *Coordinator) startPeriodicChecks() {

	ticker := time.NewTicker(10 * time.Second)

	go func() {
		for {
			select {
			case <-ticker.C:
				c.checkTimeoutsAndReassignTasks()
			case <-c.done:
				ticker.Stop()
				return
			}
		}
	}()
}

func (c *Coordinator) checkTimeoutsAndReassignTasks() {
	fmt.Printf("coordinator: checking for timed out tasks\n")
	c.mu.Lock()
	defer c.mu.Unlock()

	for i, task := range c.MapTasks {
		if task.assigned && !task.completed && time.Since(task.assignedAt) > 10*time.Second {
			fmt.Printf("coordinator: map task %v timed out, reassigning\n", i)
			c.MapTasks[i] = Task{}
		}
	}

	for i, task := range c.ReduceTasks {
		if task.assigned && !task.completed && time.Since(task.assignedAt) > 10*time.Second {
			fmt.Printf("coordinator: reduce task %v timed out, reassigning\n", i)
			c.ReduceTasks[i] = Task{}
		}
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Files:       files,
		NReduce:     nReduce,
		NMap:        len(files),
		MapTasks:    make([]Task, len(files)),
		ReduceTasks: make([]Task, nReduce),
		done:        make(chan struct{}),
	}
	fmt.Printf("Coordinator: MakeCoordinator\n")
	fmt.Printf("Coordinator: files %v\n", files)
	fmt.Printf("Coordinator: map tasks %v\n", c.MapTasks)
	fmt.Printf("Coordinator: reduce tasks %v\n", c.ReduceTasks)

	c.startPeriodicChecks()
	c.server()
	return &c
}
