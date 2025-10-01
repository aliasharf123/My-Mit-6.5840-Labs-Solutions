package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mapStatus         map[string]int
	cacheMapJobsDone  bool
	mapTaskId         int
	reduceStatus      map[int]int
	nReducer          int
	intermediateFiles map[int][]string
	mu                sync.Mutex
}

func (c *Coordinator) RequestMapJob(arg *int, reply *RequestTaskReply) error {

	c.mu.Lock()
	defer c.mu.Unlock()
	// Start with done values until find remaining job
	reply.Done = true

	if c.cacheMapJobsDone {
		return nil
	}
	for k, v := range c.mapStatus {
		switch v {
		case 0:
			reply.MapJob = &MapJob{
				InputFile:    k,
				MapJobNumber: c.mapTaskId,
				ReduceCount:  c.nReducer,
			}
			c.mapTaskId++

			// recovery from worker failure
			go func(filename string) {
				time.Sleep(10 * time.Second)
				c.mu.Lock()
				if c.mapStatus[filename] == 1 {
					c.mapStatus[filename] = 0
				}
				c.mu.Unlock()
			}(k)
			// set map as running
			c.mapStatus[k] = 1
			reply.Done = false
			return nil
		case 1:
			reply.Done = false
		}
	}

	if reply.Done {
		c.cacheMapJobsDone = true
	}

	return nil
}

func (c *Coordinator) RequestReduceJob(arg *int, reply *RequestTaskReply) error {
	c.mu.Lock()

	defer c.mu.Unlock()

	for k, v := range c.reduceStatus {
		if v == 0 {
			reply.ReduceJob = &ReduceJob{
				IntermediateFiles: c.intermediateFiles[k],
				ReduceNumber:      k,
			}

			go func(reduceNumber int) {
				time.Sleep(10 * time.Second)
				c.mu.Lock()
				if c.reduceStatus[reduceNumber] == 1 {
					c.reduceStatus[reduceNumber] = 0
				}
				c.mu.Unlock()
			}(k)

			// set reduce as running
			c.reduceStatus[k] = 1
			return nil
		}
	}
	return nil
}

// Reporting to coordinator
func (c *Coordinator) ReportMapJob(args *ReportMapTaskArgs, reply *int) error {

	c.mu.Lock()

	defer c.mu.Unlock()

	// fmt.Println("InputFile of map report:", args.InputFile)
	// fmt.Println("intermediate files of map report:", args.IntermediateFile)

	for _, v := range args.IntermediateFile {
		lastIndex := len(v) - 1
		reduceNumber, err := strconv.Atoi(string(v[lastIndex]))
		if err != nil {
			return errors.New("string isn't valid formate")
		}
		c.intermediateFiles[reduceNumber] = append(c.intermediateFiles[reduceNumber], v)
	}
	// Map job is done
	c.mapStatus[args.InputFile] = 2

	// fmt.Println("intermediate file after map report:", c.intermediateFiles)
	return nil
}
func (c *Coordinator) ReportReduceJob(args *ReportReduceTaskArgs, reply *int) error {

	c.mu.Lock()

	defer c.mu.Unlock()

	c.reduceStatus[args.ReduceNumber] = 2
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
	ret := true

	for _, v := range c.reduceStatus {
		if v != 2 {
			ret = false
		}
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReducer:          nReduce,
		mapStatus:         make(map[string]int),
		intermediateFiles: make(map[int][]string),
		mapTaskId:         0,
		reduceStatus:      make(map[int]int),
	}

	// Initialize status with 0 (not started)
	for _, file := range files {
		c.mapStatus[file] = 0
	}
	for i := range nReduce {
		c.reduceStatus[i] = 0
	}

	c.server()
	return &c
}
