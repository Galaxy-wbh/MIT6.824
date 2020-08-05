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

//任务类型常量
const (
	TYPE_MAPTASK = 1
	TYPE_REDUCETASK = 2
)


//注册worker的数据结构
type RegisterArgs struct {

}

type RegisterReply struct {
	WorkerId int
}

//返回任务的数据结构
type PullTaskArgs struct{
	WorkerId int
}
type PullTaskReply struct {
	MTask MapTask
	RTask ReduceTask
	IsMapTask bool //true: MapTask false: ReduceTask 
}

type ReportTaskArgs struct{
	MTask MapTask
	RTask ReduceTask
	Done bool 
	IsMap bool
}

type ReportTaskReply struct {

}



// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
