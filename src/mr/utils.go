package mr

import (
	"fmt"
	"log"
)

const Debug = true
const (
	//任务的状态
	TASK_IDEL = 0
	TASK_RUNNING = 1
	TASK_FINISHED = 2
	
	//MASTER的状态
	PHASE_INIT = 0
	PHASE_MAP = 1
	PHASE_REDUCE =2
)




type MapTask struct{
	Id int
	FileName string
	WorkerId int
	Status int
	NReduce int
}

type ReduceTask struct{
	Id int
	WorkerId int
	Status int
	NMap int
}


func DebugPrintf(format string, v ...interface{})  {
	if Debug {
		log.Printf(format + "\n", v...)
	}
}

func reduceName(mapId, reduceId int) string{
	return fmt.Sprintf("mr-%d-%d", mapId, reduceId)
}

func mergeName(reduceId int) string{
	return fmt.Sprintf("mr-out-%d", reduceId)
}
