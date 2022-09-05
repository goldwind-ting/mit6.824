package kvraft

import (
	"bytes"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType   string
	Key      string
	Value    string
	Id       int64
	ClientId int64
	Rid      int64
}

type ValueWrap struct {
	Value string
	Err   Err
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db        map[string]string
	history   map[int64]int64
	chs       map[int64]chan ValueWrap
	lastApply int
	stopCh    chan struct{}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		OpType: "GET",
		Key:    args.Key,
		Value:  "",
		Rid:    nrand(),
	}
	ch := make(chan ValueWrap, 1)
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	kv.chs[op.Rid] = ch
	kv.mu.Unlock()
	select {
	case v := <-ch:
		reply.Value = v.Value
		reply.Err = v.Err
	case <-time.After(time.Millisecond * 500):
		reply.Err = ErrTimeout
	}
	kv.mu.Lock()
	delete(kv.chs, op.Rid)
	kv.mu.Unlock()
}

func (kv *KVServer) existed(cid, mid int64) bool {
	if v, ok := kv.history[cid]; ok && v == mid {
		return true
	}
	return false
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	var op Op
	if args.Op == "Put" {
		op = Op{
			OpType:   "PUT",
			Key:      args.Key,
			Value:    args.Value,
			Id:       args.MsgId,
			ClientId: args.ClientId,
			Rid:      nrand(),
		}
	} else if args.Op == "Append" {
		op = Op{
			Rid:      nrand(),
			OpType:   "APPEND",
			Key:      args.Key,
			Value:    args.Value,
			Id:       args.MsgId,
			ClientId: args.ClientId,
		}
	}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	ch := make(chan ValueWrap, 1)
	kv.chs[op.Rid] = ch
	kv.mu.Unlock()
	select {
	case v := <-ch:
		reply.Err = v.Err
	case <-time.After(time.Millisecond * 500):
		reply.Err = ErrTimeout
	}
	kv.mu.Lock()
	delete(kv.chs, op.Rid)
	kv.mu.Unlock()
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	close(kv.stopCh)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) handleCommand(op Op, ix int) {
	if op.OpType == "GET" {
		kv.mu.Lock()
		kv.lastApply = ix
		if c, ok := kv.chs[op.Rid]; ok {
			if value, ok := kv.db[op.Key]; ok {
				c <- ValueWrap{
					Value: value,
					Err:   OK,
				}
			} else {
				c <- ValueWrap{
					Value: "",
					Err:   ErrNoKey,
				}
			}

		}
		kv.mu.Unlock()
	} else if op.OpType == "APPEND" {
		kv.mu.Lock()
		kv.lastApply = ix
		if !kv.existed(op.ClientId, op.Id) {
			kv.history[op.ClientId] = op.Id
			v := kv.db[op.Key]
			kv.db[op.Key] = strings.Join([]string{v, op.Value}, "")
		}
		if c, ok := kv.chs[op.Rid]; ok {
			c <- ValueWrap{
				Value: "",
				Err:   OK,
			}
		}
		kv.mu.Unlock()
	} else if op.OpType == "PUT" {
		kv.mu.Lock()
		kv.lastApply = ix
		if !kv.existed(op.ClientId, op.Id) {
			kv.history[op.ClientId] = op.Id
			kv.db[op.Key] = op.Value
		}
		if c, ok := kv.chs[op.Rid]; ok {
			c <- ValueWrap{
				Value: "",
				Err:   OK,
			}
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) makeSnapshot() {
	if kv.exceedThreshold() {
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		kv.mu.Lock()
		e.Encode(kv.db)
		e.Encode(kv.history)
		b := w.Bytes()
		kv.rf.Snapshot(kv.lastApply, b)
		kv.mu.Unlock()
	}
}

func (kv *KVServer) exceedThreshold() bool {
	return kv.maxraftstate > 0 && kv.rf.Persister().RaftStateSize() > kv.maxraftstate
}

func (kv *KVServer) Listen() {
	for {
		select {
		case m := <-kv.applyCh:
			if !m.CommandValid && m.SnapshotValid {
				kv.mu.Lock()
				r := bytes.NewBuffer(kv.rf.Persister().ReadSnapshot())
				d := labgob.NewDecoder(r)
				d.Decode(&kv.db)
				d.Decode(&kv.history)
				kv.lastApply = m.SnapshotIndex
				kv.mu.Unlock()
			} else if m.CommandValid {
				op := m.Command.(Op)
				kv.handleCommand(op, m.CommandIndex)
				kv.makeSnapshot()
			}
		case <-kv.stopCh:
			return
		}

	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.db = make(map[string]string)
	kv.history = make(map[int64]int64)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.stopCh = make(chan struct{}, 1)
	b := persister.ReadSnapshot()
	if len(b) > 0 {
		r := bytes.NewBuffer(b)
		d := labgob.NewDecoder(r)
		d.Decode(&kv.db)
		d.Decode(&kv.history)
	}
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.chs = make(map[int64]chan ValueWrap)
	// You may need initialization code here.
	go kv.Listen()
	return kv
}
