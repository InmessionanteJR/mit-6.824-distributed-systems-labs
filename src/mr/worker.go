package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
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

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func mapJob(mapf func(string, string) []KeyValue, coReply CoordinatorReply) {
	intermediate := []KeyValue{}
	filename := coReply.InputFile
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	defer file.Close()
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)
	// sort.Sort(ByKey(intermediate))

	intermediateMap := make(map[int][]KeyValue)
	for _, pair := range intermediate {
		index := ihash(pair.Key) % coReply.NReduce
		intermediateMap[index] = append(intermediateMap[index], pair)
	}
	tempNameMapping := make(map[string]string)
	for i := range intermediateMap {
		filename := fmt.Sprintf("mr-%d-%d", coReply.TaskNumber, i)
		tempFile, err := ioutil.TempFile("./tmp_map", filename+"-")
		if err != nil {
			log.Fatalf("cannot create a temporary file for %v:%s", filename, err)
		}
		defer tempFile.Close()

		tempName := tempFile.Name()
		tempNameMapping[tempName] = fmt.Sprintf("./tmp_map/mr-%d-%d", coReply.TaskNumber, i)

		enc := json.NewEncoder(tempFile)
		for _, kv := range intermediateMap[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot encode to %v", filename)
			}
		}
	}

	err = NotifyCurrentMapJobFinished(coReply.TaskNumber)
	if err != nil {
		log.Println(err)
		return
	}

	for tempName, filename := range tempNameMapping {
		err = os.Rename(tempName, filename)
		if err != nil {
			log.Fatalf("cannot rename %v", tempName)
		}
	}
}

func reduceJob(reducef func(string, []string) string, coReply CoordinatorReply) {
	intermediate := make([]KeyValue, 0)
	target := coReply.TaskNumber
	print := make(map[int]int)
	for i := 0; i < coReply.NFile; i++ {
		filename := fmt.Sprintf("./tmp_map/mr-%d-%d", i, target)
		file, err := os.Open(filename)
		if err != nil {
			// https://blog.csdn.net/qq_42553836/article/details/122745641#:~:text=map%E7%BB%93%E6%9E%9C%E5%8F%AF%E8%83%BD%E7%A1%AE%E5%AE%9E%E6%B2%A1%E6%9C%89%E4%BA%A7%E7%94%9F%E8%AF%A5reduce%E7%9A%84%E6%95%B0%E6%8D%AE
			// log.Fatalf("cannot open file %v", filename)
			continue // vital
		}
		defer file.Close()
		print[i] = target

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))

	log.Println("here!")

	filename := fmt.Sprintf("mr-out-%d-%d", target, os.Getpid())
	tempFile, err := ioutil.TempFile("./tmp_reduce", filename+"-")
	if err != nil {
		log.Fatalf("cannot create a temporary file for %v:%s", filename, err)
	}
	defer tempFile.Close()

	// call Reduce on each distinct key in intermediate[]
	// and print the result to mr-out-i
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values) // in crash test, reduce jobs may sleep for up to 10 seconds or even sleep here

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	err = NotifyCurrentReduceJobFinished(coReply.TaskNumber)
	if err != nil {
		log.Fatalf("%v", err)
		return
	}
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		coReply, err := AskForTask()
		if err != nil {
			log.Println("Coordinate has finished, so quit current process, ", err)
			return
		}
		if coReply.TaskType == "map" {
			mapJob(mapf, coReply)
		} else if coReply.TaskType == "reduce" {
			reduceJob(reducef, coReply)
		} else if coReply.TaskType == "wait" {
			time.Sleep(time.Second)
		} else {
			log.Fatal("unsupported task:", coReply.TaskType)
			return
		}
	}
}

func AskForTask() (CoordinatorReply, error) {
	workerRequest := WorkerRequest{}
	workerRequest.WorkerPID = os.Getpid()

	coReply := CoordinatorReply{}

	ok := call("Coordinator.AssignTask", &workerRequest, &coReply)
	if ok {
		return coReply, nil
	} else {
		return coReply, fmt.Errorf("RPC call failed")
	}
}

func NotifyCurrentMapJobFinished(index int) error {
	workerRequest := WorkerRequest{}
	workerRequest.FinishedTaskNumber = index
	workerRequest.WorkerPID = os.Getpid()

	coReply := CoordinatorReply{}

	ok := call("Coordinator.FinishMapTask", &workerRequest, &coReply)
	if ok {
		return nil
	} else {
		return fmt.Errorf("RPC call failed or job aborted")
	}
}

func NotifyCurrentReduceJobFinished(TaskNumber int) error {
	workerRequest := WorkerRequest{}
	workerRequest.FinishedTaskNumber = TaskNumber
	workerRequest.WorkerPID = os.Getpid()

	coReply := CoordinatorReply{}

	ok := call("Coordinator.FinishReduceTask", &workerRequest, &coReply)
	if ok {
		return nil
	} else {
		return fmt.Errorf("RPC call failed or job aborted")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Println("dialing:", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
