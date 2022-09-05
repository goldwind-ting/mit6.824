package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
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
	for {
		if CallExample(mapf, reducef) {
			break
		}
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) bool {
	args := MRArg{}
	args.OptType = REQUEST
	reply := MRReply{}
	ok := call("Coordinator.Example", &args, &reply)
	if !ok {
		log.Println("failed to call Coordinator.Example")
		return false
	}
	switch reply.OptType {
	case EXIT:
		time.Sleep(time.Second)
		return true
	case MAP:
		fm := createFM(reply.Id)
		file, err := os.Open(reply.FileName)
		if err != nil {
			log.Fatalf("cannot open %v", reply.FileName)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", reply.FileName)
		}
		file.Close()

		kva := mapf(reply.FileName, string(content))
		offsetkvs := make([][]KeyValue, 10)
		for i := 0; i < 10; i++ {
			offsetkvs[i] = make([]KeyValue, 0)
		}
		for _, kv := range kva {
			offset := ihash(kv.Key) % 10
			offsetkvs[offset] = append(offsetkvs[offset], kv)
		}
		for ix, kvs := range offsetkvs {
			enc := json.NewEncoder(fm[ix])
			if err = enc.Encode(&kvs); err != nil {
				log.Fatalf("cannot encode json to file, error: %v", err)
			}
		}
		for _, f := range fm {
			f.Close()
		}
	case REDUCE:
		fm := readFM(reply.Id)
		kva := make([]KeyValue, 0)
		for _, f := range fm {
			dec := json.NewDecoder(f)
			var kvs []KeyValue
			err := dec.Decode(&kvs)
			if err != nil {
				if err == io.EOF {
					continue
				}
				log.Fatalf("cannot decode kv %v, filename: %s", err, f.Name())
				break
			}
			kva = append(kva, kvs...)
		}
		sort.Sort(ByKey(kva))
		oname := fmt.Sprintf("mr-out-%d", reply.Id)
		var ofile *os.File
		if _, err := os.Stat(oname); os.IsExist(err) {
			ofile, _ = os.Open(oname)
		} else {
			ofile, _ = os.Create(oname)
		}

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
			output := reducef(kva[i].Key, values)

			// fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output) uncomment this, a bug will be triggered in crash test. why??
			_, err := fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
			if err != nil {
				log.Fatalf("cannot write output %v, reply.Id %d", err, reply.Id)
			}
			i = j
		}
		ofile.Close()
	}
	if reply.OptType == MAP || reply.OptType == REDUCE {
		args := MRArg{}
		args.OptType = REPORT
		args.Type = reply.OptType
		args.Id = reply.Id
		reply := MRReply{}
		ok := call("Coordinator.Example", &args, &reply)
		if !ok {
			return false
		}
		if reply.OptType == EXIT {
			return true
		}
	}
	return false
}

func createFM(id int) map[int]*os.File {
	fm := make(map[int]*os.File, 10)
	for i := 0; i < 10; i++ {
		f, err := os.Create(fmt.Sprintf("mr-%d-%d", id, i))
		if err != nil {
			log.Fatal("failed to create file", i, err)
		}
		fm[i] = f
	}
	return fm
}

func readFM(id int) map[int]*os.File {
	fm := make(map[int]*os.File, 10)
	for i := 0; i < 10; i++ {
		fn := fmt.Sprintf("mr-%d-%d", i, id)
		if _, err := os.Stat(fn); os.IsNotExist(err) {
			continue
		}
		f, err := os.Open(fn)
		if err != nil {
			log.Fatal("failed to read file", i, id)
		}
		fm[i] = f
	}
	return fm
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

	log.Println("error: ", err)
	return false
}
