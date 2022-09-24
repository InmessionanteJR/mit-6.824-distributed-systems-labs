package mr

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type mapAssignedStatus struct {
	status    int // status: 0: can be assigned to a new job, 1: assigned but yet finished 2: finished
	filename  string
	jobPid    int
	timestamp time.Time
}

type reduceAssignedStatus struct {
	status    int // status: 0: can be assigned to a new job, 1: assigned but yet finished 2: finished
	jobPid    int
	timestamp time.Time
}

type Coordinator struct {
	mapTask             []mapAssignedStatus
	reduceTask          []reduceAssignedStatus
	mapDoneChan         []chan bool
	reduceDoneChan      []chan bool
	NumFinishMapTask    int
	NumFinishReduceTask int
	nFile               int
	nReduce             int
	mu                  sync.Mutex
}

func (c *Coordinator) FinishMapTask(recv *WorkerRequest, reply *CoordinatorReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// status 0 or 2 both means the current job should be aborted
	if c.mapTask[recv.FinishedTaskNumber].status == 1 && c.mapTask[recv.FinishedTaskNumber].jobPid == recv.WorkerPID && time.Now().Add(-10*time.Second).Before(c.mapTask[recv.FinishedTaskNumber].timestamp) {
		c.mapDoneChan[recv.FinishedTaskNumber] <- true
		return nil
	}
	return fmt.Errorf("aborted")
}

func (c *Coordinator) FinishReduceTask(recv *WorkerRequest, reply *CoordinatorReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// status 0 or 2 both means the current job should be aborted
	if c.reduceTask[recv.FinishedTaskNumber].status == 1 && c.reduceTask[recv.FinishedTaskNumber].jobPid == recv.WorkerPID && time.Now().Add(-10*time.Second).Before(c.reduceTask[recv.FinishedTaskNumber].timestamp) {
		c.reduceDoneChan[recv.FinishedTaskNumber] <- true
		return nil
	}
	return fmt.Errorf("aborted")
}

func (c *Coordinator) monitorMapJob(index int) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	select {
	case <-c.mapDoneChan[index]:
		c.mu.Lock()
		c.mapTask[index].status = 2
		c.NumFinishMapTask += 1
		c.mu.Unlock()
	case <-ctx.Done():
		c.mu.Lock()
		c.mapTask[index].jobPid = -1
		c.mapTask[index].status = 0
		c.mapTask[index].timestamp = time.Time{}
		c.mu.Unlock()
	}
}

func renameReduceFile(index int, jobPid int) {
	files, _ := ioutil.ReadDir("./tmp_reduce")
	for _, f := range files {
		tmp := strings.Split(f.Name(), "-")
		if tmp[3] == strconv.Itoa(jobPid) && tmp[2] == strconv.Itoa(index) {
			err := os.Rename("./tmp_reduce/"+f.Name(), strings.Join(tmp[:len(tmp)-2], "-"))
			if err != nil {
				log.Fatalf("cannot rename %v:%s", f.Name(), err)
			}
			log.Printf("rename: %s -> %s", "./tmp_reduce"+f.Name(), strings.Join(tmp[:len(tmp)-2], "-"))
		}
	}
}

func (c *Coordinator) monitorReduceJob(index int) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	select {
	case <-c.reduceDoneChan[index]:
		c.mu.Lock()
		c.reduceTask[index].status = 2
		c.NumFinishReduceTask += 1
		renameReduceFile(index, c.reduceTask[index].jobPid)
		c.mu.Unlock()
	case <-ctx.Done():
		c.mu.Lock()
		c.reduceTask[index].jobPid = -1
		c.reduceTask[index].status = 0
		c.reduceTask[index].timestamp = time.Time{}
		c.mu.Unlock()
	}
}

func (c *Coordinator) AssignTask(recv *WorkerRequest, reply *CoordinatorReply) error {
	reply.TaskType = "wait"

	c.mu.Lock()
	defer c.mu.Unlock()

	// map
	if c.NumFinishMapTask < c.nFile {
		for index, val := range c.mapTask {
			if val.status == 0 {
				reply.InputFile = val.filename
				reply.TaskType = "map"
				reply.TaskNumber = index
				reply.NReduce = c.nReduce
				c.mapTask[index].status = 1 // assigned
				c.mapTask[index].jobPid = recv.WorkerPID
				c.mapTask[index].timestamp = time.Now()
				go c.monitorMapJob(index)
				return nil
			}
		}
		// wait
		reply.TaskType = "wait"
		return nil
	}

	// reduce
	if c.NumFinishReduceTask < c.nReduce {
		for index, val := range c.reduceTask {
			if val.status == 0 {
				reply.TaskType = "reduce"
				reply.TaskNumber = index
				reply.NFile = c.nFile
				c.reduceTask[index].status = 1 // assigned
				c.reduceTask[index].jobPid = recv.WorkerPID
				c.reduceTask[index].timestamp = time.Now()
				go c.monitorReduceJob(index)
				return nil
			}
		}
		// wait
		reply.TaskType = "wait"
		return nil
	}

	return nil
}

func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	// start a thread that listens for RPCs from worker.go
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, val := range c.reduceTask {
		if val.status != 2 {
			return false
		}
	}
	// time.Sleep(time.Second) // give the coordinator time to write to final output file, vital
	for i := 0; i < 10; i++ {
		log.Printf("c.reduceTask[%d]:%d ", i, c.reduceTask[i])
	}

	return true
}

func clearAndReCreateFolder() {
	// delete ./tmp and create an empty ./tmp
	err := os.RemoveAll("./tmp_map")
	if err != nil {
		log.Fatal(err)
	}
	err = os.RemoveAll("./tmp_reduce")
	if err != nil {
		log.Fatal(err)
	}
	if err := os.Mkdir("./tmp_map", os.ModePerm); err != nil {
		log.Fatal(err)
	}
	if err := os.Mkdir("./tmp_reduce", os.ModePerm); err != nil {
		log.Fatal(err)
	}

	// delete all mr-out-*
	files, err := filepath.Glob("mr-out-*")
	if err != nil {
		panic(err)
	}
	for _, f := range files {
		if err := os.Remove(f); err != nil {
			panic(err)
		}
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	clearAndReCreateFolder()
	c := Coordinator{
		mapTask:        make([]mapAssignedStatus, len(files)),
		reduceTask:     make([]reduceAssignedStatus, nReduce),
		mapDoneChan:    make([]chan bool, len(files)),
		reduceDoneChan: make([]chan bool, nReduce),
		nFile:          len(files),
		nReduce:        nReduce,
	}
	log.Println(files)

	for index, filename := range files {
		c.mapTask[index].filename = filename
	}
	for i := 0; i < c.nFile; i++ {
		c.mapDoneChan[i] = make(chan bool, 1)
	}
	for i := 0; i < c.nReduce; i++ {
		c.reduceDoneChan[i] = make(chan bool, 1)
	}

	c.server()
	return &c
}
