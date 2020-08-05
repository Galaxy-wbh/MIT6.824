package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
//import "fmt"
import "time"



type Master struct {
	// Your definitions here.
	NReduce int
	NMap int

	MapTasks []*MapTask
	ReduceTasks []*ReduceTask
	
	State int //0:master_init 1:map_finished 2:reduce_finished
	
	MapTaskChan chan *MapTask
	ReduceTaskChan chan *ReduceTask

	WorkerSeq int
	//IntermediateFiles [][]string

	NFinishedMap int
	NFinishedReduce int

	done bool
	mu sync.Mutex

}



// Your code here -- RPC handlers for the worker to call.

func (m *Master) AssignTask(args *PullTaskArgs, reply *PullTaskReply) error{
	select {
		case mapTask := <- m.MapTaskChan:
			DebugPrintf("Master assign a map task: ")
			mapTask.Status = TASK_RUNNING
			mapTask.WorkerId = args.WorkerId
			reply.IsMapTask = true
			reply.MTask = *mapTask
			go m.MonitorMapTask(mapTask)
		case reduceTask := <- m.ReduceTaskChan:
			DebugPrintf("Master assign a reduce task: ")
			reduceTask.Status = TASK_RUNNING
			reply.IsMapTask = false
			reply.RTask = *reduceTask
			go m.MonitorReduceTask(reduceTask)
	}
	return nil
}


func (m *Master) MonitorMapTask(task *MapTask){
	t := time.NewTicker(time.Second * 10)
	defer t.Stop()
	for {
		select {
			case <- t.C:
				m.mu.Lock()
				task.Status = TASK_IDEL
				m.MapTaskChan <- task
				m.mu.Unlock()
			default:
				if task.Status == TASK_FINISHED {
					return
				}
		}
	}
}
func (m *Master) MonitorReduceTask(task *ReduceTask){
	t := time.NewTicker(time.Second * 10)
	defer t.Stop()
	for {
		select {
			case <- t.C:
				m.mu.Lock()
				task.Status = TASK_IDEL
				m.ReduceTaskChan <- task
				m.mu.Unlock()
			default:
				if task.Status == TASK_FINISHED {
					return
				}
		}
	}
}



func (m *Master) ReportTaskFinish(args *ReportTaskArgs, reply *ReportTaskReply) error{
	if args.IsMap {
		if args.Done {
			DebugPrintf("MapTask %d finished.\n", args.MTask.Id)
			m.mu.Lock()
			m.MapTasks[args.MTask.Id].Status = TASK_FINISHED
			m.NFinishedMap++
			if m.NFinishedMap == m.NMap {
				DebugPrintf("All MapTask is done!\n")
				m.State = PHASE_REDUCE
				go m.generateReduceTask()
			}
			m.mu.Unlock()
		} else {
			//m.MapTasks[args.MTask.Id].Status = TASK_IDEL
		}
		
	} else {
		DebugPrintf("ReduceTask %d finished.\n", args.RTask.Id)
		m.mu.Lock()
		defer m.mu.Unlock()
		m.ReduceTasks[args.RTask.Id].Status = TASK_FINISHED
		m.NFinishedReduce++
		if m.NFinishedReduce == m.NReduce {
			DebugPrintf("All ReduceTask is done!\n")
			m.done = true
		}
	}
	return nil
}

func (m *Master) generateMapTask(files []string){
	DebugPrintf("Generate Map Tasks...")
	for i, fileName := range files {
		mapTask := MapTask{
			Id: i,
			FileName: fileName,
			Status: TASK_IDEL,
			NReduce: m.NReduce,
		}
		m.MapTasks = append(m.MapTasks, &mapTask)
		m.MapTaskChan <- (&mapTask)
	}
	m.State = PHASE_MAP
}

func (m *Master) generateReduceTask(){
	DebugPrintf("Generate Reduce Tasks...")
	for i := 0; i < m.NReduce; i++ {
		reduceTask := ReduceTask{
			Id: i,
			Status: TASK_IDEL,
			NMap: m.NMap,
		}
		m.ReduceTasks = append(m.ReduceTasks, &reduceTask)
		m.ReduceTaskChan <- (&reduceTask)
	}
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Your code here.
	return m.done
}

func (m *Master) RegisterWorker(args *RegisterArgs, reply *RegisterReply) error{
	m.mu.Lock()
	defer m.mu.Unlock()
	m.WorkerSeq += 1
	reply.WorkerId = m.WorkerSeq
	return nil
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	
	m := Master{
		NReduce: nReduce,
		NMap: len(files),	
		State: PHASE_INIT,
		WorkerSeq: 0,
		MapTaskChan: make(chan *MapTask),
		ReduceTaskChan: make(chan *ReduceTask),
		//IntermediateFiles: make([][]string, nReduce),
		NFinishedMap: 0,
		NFinishedReduce: 0,
		done: false,
	}
	m.server()
	// Your code here.
	m.generateMapTask(files)

	return &m
}
