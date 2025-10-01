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

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// json Data formate
type IntermediateJsonFile struct {
	Data []KeyValue
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go provided function to sort kvacalls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	flagMapJobLeft := true

	// 0.0 Loop with these steps until coordinator stop responding then exit the worker
	for {
		time.Sleep(time.Millisecond * 400)
		// 1. Call for Map Job.
		if flagMapJobLeft {
			reply := &RequestTaskReply{}
			if err := call("Coordinator.RequestMapJob", 1, reply); !err {
				log.Fatal("Something went wrong in map job")
				return
			}
			if reply.Done {
				flagMapJobLeft = false
			} else {
				if reply.MapJob != nil {
					if mapJobErr := handleMapJob(mapf, reply); mapJobErr {
						break
					}
				}
				continue
			}
		}
		// 2. if reply with nil then Call for reduce job
		reply := &RequestTaskReply{}
		if err := call("Coordinator.RequestReduceJob", 2, reply); !err {
			log.Fatal("Something went wrong in reduce job")
			return
		}
		if reply.ReduceJob != nil {
			if err := handleReduceJob(reducef, reply); err {
				break
			}
		}
	}

}
func handleReduceJob(reducef func(string, []string) string, reply *RequestTaskReply) bool {

	reduceNumber := reply.ReduceJob.ReduceNumber
	fmt.Fprintf(os.Stdout, "Start working in (%d) reduce job\n", reduceNumber)

	// 2.1. open all intermediate fils and deserialized it
	// 2.2. build array of kva from intermediate files
	var kva []KeyValue
	// fmt.Println("intermeidate file :", reply.ReduceJob.IntermediateFiles)
	for _, v := range reply.ReduceJob.IntermediateFiles {
		file, err := os.Open(v)
		if err != nil {
			log.Fatalf("cannot open %v", v)
			return true
		}
		dec := json.NewDecoder(file)
		var intermediateJsonFile IntermediateJsonFile
		if err := dec.Decode(&intermediateJsonFile); err != nil {
			log.Fatal(err)
			return true
		}
		kva = append(kva, intermediateJsonFile.Data...)

		file.Close()
	}
	// 2.3. use the provided function to sort kva
	sort.Sort(ByKey(kva))

	// 2.4. Combine all values of same key like "A", ["1", "1"] and put it to reducef
	tmpName := fmt.Sprintf("temp-%d", reduceNumber)
	tmp, err := os.CreateTemp("", tmpName)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return true
	}
	defer tmp.Close()
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
		// 2.5. Every time a reducef output a result, we can persist it into a temp file.
		output := reducef(kva[i].Key, values)
		outputLine := fmt.Sprintf("%v %v\n", kva[i].Key, output)
		if _, err = tmp.WriteString(outputLine); err != nil {
			panic(err)
		}

		i = j

	}
	// 2.5. once all keys are reduced we can rename the temp file the final output file mr-out-x, where x is the reducer number
	oname := fmt.Sprintf("mr-out-%d", reduceNumber)

	if err := os.Rename(tmp.Name(), oname); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return true
	}

	// 2.6 Report the coordinator that the reduce job is finished
	if err := call("Coordinator.ReportReduceJob", &ReportReduceTaskArgs{
		ReduceNumber: reduceNumber,
	}, 1); err {
		log.Fatal("Something went wrong in report reduce job")
		return true
	}

	return false
}

func handleMapJob(mapf func(string, string) []KeyValue,
	reply *RequestTaskReply) bool {
	// 1.1. Open input file.
	// 1.2. Apply mapf to file. `return` key value pairs `kva`
	filename := reply.MapJob.InputFile
	fmt.Fprintf(os.Stdout, "Start working in %s file's map job\n", filename)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return true
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return true
	}
	file.Close()
	kva := mapf(filename, string(content))
	// 1.3. iterate over kva array to decide which key belongs to partition number
	reduceCount := reply.MapJob.ReduceCount
	partitionedKva := make([][]KeyValue, reduceCount)
	for _, v := range kva {
		partitionKey := ihash(v.Key) % reduceCount
		partitionedKva[partitionKey] = append(partitionedKva[partitionKey], v)
	}
	var intermediateFiles []string
	for k, v := range partitionedKva {
		// 1.4. Create file and save files for each partition kva with filename `mr-x-i` where `x` is the map job number
		oname := fmt.Sprintf("mr-%d-%d", reply.MapJob.MapJobNumber, k)
		ofile, _ := os.Create(oname)
		defer ofile.Close()
		// 1.5. turn the partitionedKva[i] to json formate
		enc := json.NewEncoder(ofile)
		if err := enc.Encode(IntermediateJsonFile{Data: v}); err != nil {
			fmt.Println("Error encoding JSON:", err)
			return true
		}
		intermediateFiles = append(intermediateFiles, oname)
	}
	// fmt.Println("intermediate files before it get reported", intermediateFiles)
	// 1.6. after finishing report coordinator a job is completed
	if requestErr := call("Coordinator.ReportMapJob", &ReportMapTaskArgs{
		InputFile:        filename,
		IntermediateFile: intermediateFiles,
	}, 1); requestErr {
		log.Fatal("Something went wrong")
		return true
	}
	return false
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
