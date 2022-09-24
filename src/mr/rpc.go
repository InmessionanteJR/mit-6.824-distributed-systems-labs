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

type CoordinatorRequest struct {
}

type CoordinatorReply struct {
	TaskType   string
	TaskNumber int
	InputFile  string
	NReduce    int
	NFile      int
}

type WorkerRequest struct {
	FinishedTaskNumber int
	WorkerPID          int
}

type WorkerReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	// unix domain socket in Go: https://zhuanlan.zhihu.com/p/426644841
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
