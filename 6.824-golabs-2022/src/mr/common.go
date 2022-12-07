package mr

import (
	"strconv"
	"time"
)

type TaskStatus string
type TaskState string

const (
	Waiting  TaskStatus = "Waiting"
	Running  TaskStatus = "Running"
	Complete TaskStatus = "Complete"
)

const (
	Map    TaskState = "Map"
	Reduce TaskState = "Reduce"
	Wait   TaskState = "Wait"
	Exit   TaskState = "Exit"
)

type TaskStats struct {
	startTime     time.Time  // 运行的初始时间
	workerId      int        // 在哪台机器上运行
	taskStatus    TaskStatus // 任务的状态 "Wait" "Running" "Complete"
	taskReference *Task
}

type Task struct {
	TaskState TaskState // Map Reduce
	TaskId    int       // 任务的编号
	FileName  string
	NReduce   int
	NMap      int
}

// 表示第 i 个 map task 转换给第 j 个 reduce 任务处理
func GenIntermediatesName(i, j int) string {
	return "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(j)
}

func GenTemOutFile(idx int) string {
	return "mr-out-" + strconv.Itoa(idx)
}
