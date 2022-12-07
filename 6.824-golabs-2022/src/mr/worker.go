package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type worker struct {
	id      int
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

// main/mrworker.go calls this function. 主函数
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	w := worker{}
	w.mapf = mapf
	w.reducef = reducef
	w.registerWorker()
	w.run()
}

func (w *worker) registerWorker() {
	args := &RegisterArgs{}
	reply := &RegisterReply{}
	if ok := call("Master.RegisterWorker", args, reply); !ok {
		log.Fatal("Master.RegisterWorker\n")
		panic("Master.RegisterWorker")
	}
	w.id = reply.WorkerId
}

func (w *worker) run() {
	for {
		args := &TaskArgs{
			WorkerId: w.id,
		}
		reply := &TaskReply{}
		if ok := call("Master.GetOneTask", args, reply); !ok {
			log.Fatal("Master.GetOneTask Fail\n")
			panic("Master.GetOneTask")
		}

		if reply.Task.TaskState == Map {
			// log.Printf("Worker id : %v is doing MapTask.", w.id)
			w.Map(reply.Task)
		} else if reply.Task.TaskState == Reduce {
			// log.Printf(" Worker id : %v is doing ReduceTask.", w.id)
			w.Reduce(reply.Task)
		} else if reply.Task.TaskState == Wait {
			// log.Printf("Wait for 5 seconds.")
			time.Sleep(5 * time.Second)
		} else if reply.Task.TaskState == Exit {
			return
		}
	}
}

func (w *worker) ReportTask(t *Task) {
	args := &ReportTaskArgs{
		Task: t,
	}
	reply := &ReportTaskReply{}
	if ok := call("Master.ReportTask", args, reply); !ok {
		log.Fatal("Master.ReportTask Fail\n")
		panic("Master.ReportTask")
	}
}

func (w *worker) Map(t *Task) {
	file, err := os.Open(t.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", t.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", t.FileName)
	}

	kvs := w.mapf(t.FileName, string(content))
	intermediates := make([][]KeyValue, t.NReduce)
	for _, kv := range kvs {
		hashid := ihash(kv.Key) % t.NReduce
		intermediates[hashid] = append(intermediates[hashid], kv)
	}

	// WriteToLocalFile 导出文件到本地
	for i := 0; i < t.NReduce; i++ {
		localWriteFile, _ := os.Create(GenIntermediatesName(t.TaskId, i))
		for _, kv := range intermediates[i] {
			fmt.Fprintf(localWriteFile, "%v\t%v\n", kv.Key, kv.Value)
		}
		// log.Printf("write to localFile: %v-%v success!", t.TaskId, i)
		localWriteFile.Close()
	}

	// 完成任务，进行上报
	w.ReportTask(t)
}

func (w *worker) Reduce(t *Task) {
	kva := []KeyValue{}

	for i := 0; i < t.NMap; i++ {
		remoteReadFile, err := os.Open(GenIntermediatesName(i, t.TaskId))
		if err != nil {
			log.Fatalf("cannot open %v", t.FileName)
		}
		content, err := ioutil.ReadAll(remoteReadFile)
		if err != nil {
			log.Fatalf("cannot read %v", t.FileName)
		}
		lines := strings.Split(string(content), "\n")
		for _, line := range lines {
			if strings.TrimSpace(line) == "" {
				continue
			}
			kv := strings.Split(line, "\t")
			kva = append(kva, KeyValue{Key: kv[0], Value: kv[1]})
		}
	}

	sort.Sort(ByKey(kva))
	ofile, _ := os.Create("mr-out-" + strconv.Itoa(t.TaskId))

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := w.reducef(kva[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		i = j
	}

	// 完成任务，进行上报
	w.ReportTask(t)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSocket()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
		os.Exit(0)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
