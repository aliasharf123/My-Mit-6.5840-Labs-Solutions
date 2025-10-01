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

// Add your RPC definitions here.

// Map Jop Request Argument
type MapJob struct {
	InputFile    string
	MapJobNumber int
	ReduceCount  int // Decide the key partition
}

// Type of Reduce jobs after all map jobs finished
type ReduceJob struct {
	IntermediateFiles []string // are the intermediate files for the reduce number `ReduceNumber`
	ReduceNumber      int
}

// The Arguments reported to server after the map job is finished
type ReportMapTaskArgs struct {
	InputFile        string
	IntermediateFile []string
}

type ReportReduceTaskArgs struct {
	ReduceNumber int
}

type RequestTaskReply struct {
	MapJob    *MapJob
	ReduceJob *ReduceJob
	Done      bool // Indicate if the tasks requested are already finished wither they are map or reduce
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
