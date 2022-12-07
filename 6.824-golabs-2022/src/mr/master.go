package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Stage int64

const (
	MapStage    Stage = 0
	ReduceStage Stage = 1
	Done        Stage = 2
) // 当前 Master 所处于的阶段。

type Master struct {
	// Your definitions here.
	workerSeq  int                // worker 的编号
	mutex      sync.Mutex         // 对任务队列进行取任务 / 添加任务的时候需要上锁
	taskStatus map[int]*TaskStats // 所有task的信息 taskId -> TaskStats
	taskQueue  chan *Task         // 任务队列
	nMap       int                // map 的数量
	nReduce    int                // nReduce is the number of reduce tasks to use
	filenames  []string           // 文件名字
	stage      Stage              // 当前处于的状态
}

func (m *Master) RegisterWorker(args *RegisterArgs, reply *RegisterReply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.workerSeq += 1
	reply.WorkerId = m.workerSeq
	// log.Printf("Register worker: %v\n", m.workerSeq)
	return nil
}

func (m *Master) GetOneTask(args *TaskArgs, reply *TaskReply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if len(m.taskQueue) > 0 {
		task := <-m.taskQueue
		m.taskStatus[task.TaskId].startTime = time.Now()
		m.taskStatus[task.TaskId].workerId = args.WorkerId
		m.taskStatus[task.TaskId].taskStatus = Running
		reply.Task = task
		// log.Printf("get one Task, args: %+v, reply: %+v\n", args, reply.Task)
	} else if m.stage == MapStage {
		task := &Task{
			TaskState: Wait,
		}
		reply.Task = task
		// log.Printf("Map is finished, wait for reduce.\n")
	} else {
		task := &Task{
			TaskState: Exit,
		}
		reply.Task = task
		// log.Printf("All tasks are finished, Exit.\n")
	}
	return nil
}

func (m *Master) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if args != nil && args.Task != nil {
		task := args.Task
		m.taskStatus[task.TaskId].taskStatus = Complete
	}
	go m.checkTaskDone()
	return nil
}

func (m *Master) initMapTask() {
	// 创建 Map 任务，有多少个要处理的文件，就创建多少个 task
	m.taskStatus = make(map[int]*TaskStats)
	for idx, file := range m.filenames {
		task := &Task{
			TaskState: Map,
			TaskId:    idx,
			FileName:  file,
			NReduce:   m.nReduce,
			NMap:      m.nMap,
		}
		m.taskStatus[task.TaskId] = &TaskStats{
			taskStatus:    Waiting,
			taskReference: task,
		}
		m.taskQueue <- task
	}
}

func (m *Master) initReduceTask() {
	// 创建 Reduce 任务，有多少个中间文件，就创建多少个 task
	m.taskStatus = make(map[int]*TaskStats)
	for idx := 0; idx < m.nReduce; idx++ {
		task := &Task{
			TaskState: Reduce,
			TaskId:    idx,
			NReduce:   m.nReduce,
			NMap:      m.nMap,
		}
		m.taskStatus[task.TaskId] = &TaskStats{
			taskStatus:    Waiting,
			taskReference: task,
		}
		m.taskQueue <- task
	}
}

func (m *Master) checkTaskDone() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	switch m.stage {
	case MapStage:
		if m.allTaskDone() {
			m.initReduceTask()
			m.stage = ReduceStage
		}
	case ReduceStage:
		if m.allTaskDone() {
			m.stage = Done
		}
	}
}

func (m *Master) allTaskDone() bool {
	allTaskDone := true
	for _, taskStatus := range m.taskStatus {
		if taskStatus.taskStatus != Complete {
			allTaskDone = false
		}
	}
	return allTaskDone
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSocket()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// Your code here.
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return (m.stage == Done)
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//

/*
func (m *Master) catchTimeOut() {

}
*/

func (m *Master) catchTimeOut() {
	for {
		time.Sleep(5 * time.Second)
		m.mutex.Lock()
		if m.stage == Done {
			m.mutex.Unlock()
			return
		}
		for _, taskStats := range m.taskStatus {
			if taskStats.taskStatus == "Running" && time.Now().Sub(taskStats.startTime) > 10*time.Second {
				m.taskQueue <- taskStats.taskReference
				taskStats.taskStatus = Waiting
			}
		}
		m.mutex.Unlock()
	}
}

func max(i, j int) int {
	if i > j {
		return i
	}
	return j
}

func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		filenames: files,
		taskQueue: make(chan *Task, max(len(files), nReduce)),
		nMap:      len(files),
		nReduce:   nReduce,
	}
	// log.Printf("files names: %v\n", files)
	m.initMapTask()
	// log.Print("Master start!")
	m.server()
	// 检查超时任务
	go m.catchTimeOut()
	return &m
}
