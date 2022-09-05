package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	sync.Mutex
	OriginalFiles []string
	NReduce       int
	currentMap    int
	currentReduce int
	nMapDone      int
	nMapDoneTs    map[int][]int64
	nReduceDone   int
	nReduceDoneTs map[int][]int64
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *MRArg, reply *MRReply) error {
	c.Lock()
	defer c.Unlock()
	switch args.OptType {
	case REPORT:
		if args.Type == MAP {
			if c.nMapDoneTs[args.Id][1] > 0 {
				return nil
			}
			c.nMapDone += 1
			c.nMapDoneTs[args.Id][1] = time.Now().Unix()
		} else if args.Type == REDUCE {
			if c.nReduceDoneTs[args.Id][1] > 0 {
				return nil
			}
			c.nReduceDone += 1
			c.nReduceDoneTs[args.Id][1] = time.Now().Unix()
		}
	case REQUEST:
		if c.currentMap < len(c.OriginalFiles) {
			reply.FileName = c.OriginalFiles[c.currentMap]
			reply.OptType = MAP
			reply.Id = c.currentMap
			c.nMapDoneTs[c.currentMap][0] = time.Now().Unix()
			c.currentMap += 1
		} else if c.nMapDone != len(c.OriginalFiles) {
			k := -1
			for ik, iv := range c.nMapDoneTs {
				if iv[1] == 0 && time.Now().Unix()-iv[0] >= 10 {
					k = ik
				}
			}
			if k >= 0 {
				reply.FileName = c.OriginalFiles[k]
				reply.OptType = MAP
				reply.Id = k
				c.nMapDoneTs[k][0] = time.Now().Unix()
			}
		} else if c.currentReduce < c.NReduce {
			reply.OptType = REDUCE
			reply.Id = c.currentReduce
			c.nReduceDoneTs[c.currentReduce][0] = time.Now().Unix()
			c.currentReduce += 1
		} else {
			k := -1
			for ik, iv := range c.nReduceDoneTs {
				if iv[1] == 0 && time.Now().Unix()-iv[0] >= 10 {
					k = ik
				}
			}
			if k >= 0 {
				reply.OptType = REDUCE
				reply.Id = k
				c.nReduceDoneTs[k][0] = time.Now().Unix()
			}
		}
	}
	if c.nReduceDone >= c.NReduce {
		reply.OptType = EXIT
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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
	go http.Serve(l, nil)
}

func (c *Coordinator) Done() bool {
	ret := true

	// Your code here.
	c.Lock()
	if c.nReduceDone < c.NReduce {
		ret = false
	}
	c.Unlock()
	return ret
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		OriginalFiles: files,
		NReduce:       nReduce,
		currentMap:    0,
		currentReduce: 0,
		nMapDone:      0,
		nReduceDone:   0,
	}
	c.nMapDoneTs = make(map[int][]int64, len(files))
	for i := 0; i < len(files); i++ {
		c.nMapDoneTs[i] = []int64{0, 0}
	}

	c.nReduceDoneTs = make(map[int][]int64, nReduce)
	for i := 0; i < nReduce; i++ {
		c.nReduceDoneTs[i] = []int64{0, 0}
	}
	c.server()
	return &c
}
