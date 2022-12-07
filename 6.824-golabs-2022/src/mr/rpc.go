package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type RegisterArgs struct {
}

type RegisterReply struct {
	WorkerId int
}

type TaskArgs struct {
	WorkerId int
}

type TaskReply struct {
	Task *Task
}

type ReportTaskArgs struct {
	Task *Task
}

type ReportTaskReply struct{}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSocket() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
