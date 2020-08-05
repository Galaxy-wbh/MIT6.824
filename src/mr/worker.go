package mr

import (
		"fmt"
		"log"
		"net/rpc"
		"hash/fnv"
		"os"
		"io/ioutil"
		"encoding/json"
		"time"
		"strings"
)


//
// Map functions return a slice of KeyValue.
//

type KeyValue struct {
	Key   string
	Value string
}
//worker的数据结构
type WorkerMachine struct {
	Id int
	mapf func(string, string) []KeyValue
	reducef func(string, []string) string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	w := WorkerMachine{}
	w.mapf = mapf
	w.reducef = reducef
	w.register()
	w.run() 
}
func (w *WorkerMachine) register() {
	args := &RegisterArgs{}
	reply := &RegisterReply{}
	if call("Master.RegisterWorker", &args, &reply){
		w.Id = reply.WorkerId
		DebugPrintf("Worker's register done. Worker id:%d", w.Id)
	}else{
		DebugPrintf("register fail!")
	}
}

func (w *WorkerMachine) run() {
	for {
		reply := w.PullTask()
		time.Sleep(time.Millisecond * 500)
		if reply.IsMapTask {
			DebugPrintf("Process Map Task.")
			w.doMapTask(reply.MTask)
		} else {
			DebugPrintf("Process Reduce Task.")
			w.doRecuceTask(reply.RTask)
		}
	}
}



func (w *WorkerMachine) PullTask() *PullTaskReply{
	args := PullTaskArgs{}
	reply := PullTaskReply{}
	if call("Master.AssignTask", &args, &reply) {
		DebugPrintf("Pull Task success.")
		return &reply
	}else {
		DebugPrintf("Pull Task error")
		os.Exit(1)
		return nil
	}
}

func (w *WorkerMachine) doMapTask(task MapTask) {
	DebugPrintf("in func doMapTask")
	contents, err := ioutil.ReadFile(task.FileName)
	if err != nil {
		w.report(task, ReduceTask{}, true, false, err)
		return 
	}
	kvs := w.mapf(task.FileName, string(contents))
	reduces := make([][]KeyValue, task.NReduce)
	for _, kv := range kvs {
		idx := ihash(kv.Key) % task.NReduce
		reduces[idx] = append(reduces[idx], kv)
	}

	for idx, l := range reduces {
		fileName := reduceName(task.Id, idx)
		f, err := os.Create(fileName)
		DebugPrintf("create file:%v", fileName)
		if err != nil {
			w.report(task, ReduceTask{}, true, false, err)
			return 
		}
		enc := json.NewEncoder(f)
		for _, kv := range l {
			if err := enc.Encode(&kv); err != nil {
				w.report(task, ReduceTask{}, true, false, err)
			}
		}
		if err := f.Close(); err != nil {
			w.report(task, ReduceTask{}, true, false, err)
		}
	}
	w.report(task, ReduceTask{}, true, true, nil)
}


func (w *WorkerMachine) doRecuceTask(task ReduceTask) {
	maps := make(map[string][]string)
	for idx := 0; idx < task.NMap; idx++ {
		fileName := reduceName(idx, task.Id)
		file, err := os.Open(fileName)
		if err != nil {
			w.report(MapTask{}, task, false, false, err)
			return 
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			if _, ok := maps[kv.Key]; !ok {
				maps[kv.Key] = make([]string, 0, 100)
			}
			maps[kv.Key] = append(maps[kv.Key], kv.Value)
		}
	}
	res := make([]string, 0, 100)
	for k, v := range maps{
		res = append(res, fmt.Sprintf("%v %v\n", k, w.reducef(k, v)))
	}

	if err := ioutil.WriteFile(mergeName(task.Id), []byte(strings.Join(res, "")), 0600); err != nil {
		w.report(MapTask{}, task, false, false, err)
	}
	w.report(MapTask{}, task, true, false, nil)

}

func (w *WorkerMachine) report(mTask MapTask, rTask ReduceTask,
	 done bool, isMap bool, err error) {
	if err != nil {
		log.Printf ("%v", err)
	}
	args := ReportTaskArgs{}
	reply := ReportTaskReply{}
	args.Done = done
	args.IsMap = isMap
	if isMap {
		args.MTask = mTask
	} else {
		args.RTask = rTask
	}
	if ok := call("Master.ReportTaskFinish", &args, &reply); !ok {
		DebugPrintf("Report task fail")
	}

}


//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
