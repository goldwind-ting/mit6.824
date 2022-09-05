package shardctrler

import (
	"sort"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type ValueWrap struct {
	Value *Config
	Err   Err
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num
	history map[int64]int64
	chs     map[int64]chan ValueWrap
}

type OptType int

type Handler func(*Op) *Config

const (
	JOIN OptType = iota
	LEAVE
	MOVE
	QUERY
)

type Op struct {
	// Your data here.
	Type     OptType
	Rid      int64
	Shard    int
	ClientId int64
	MsgId    int64
	GIDs     []int
	GID      int
	Num      int
	Servers  map[int][]string
}

func (sc *ShardCtrler) send(op Op) (e Err, cf *Config) {
	c := make(chan ValueWrap, 1)
	_, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		e = ErrWrongLeader
		return
	}
	sc.mu.Lock()
	sc.chs[op.Rid] = c
	sc.mu.Unlock()
	select {
	case <-time.After(time.Millisecond * 500):
		e = ErrTimeout
	case v := <-c:
		e = v.Err
		cf = v.Value
	}
	sc.mu.Lock()
	delete(sc.chs, op.Rid)
	sc.mu.Unlock()
	return
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{
		Type:     JOIN,
		Servers:  args.Servers,
		ClientId: args.ClientId,
		MsgId:    args.MsgId,
		Rid:      nrand(),
	}
	e, _ := sc.send(op)
	reply.Err = e
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{
		Type:     LEAVE,
		GIDs:     args.GIDs,
		ClientId: args.ClientId,
		MsgId:    args.MsgId,
		Rid:      nrand(),
	}
	e, _ := sc.send(op)
	reply.Err = e
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{
		Type:     MOVE,
		GID:      args.GID,
		Shard:    args.Shard,
		ClientId: args.ClientId,
		MsgId:    args.MsgId,
		Rid:      nrand(),
	}
	e, _ := sc.send(op)
	reply.Err = e
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()
	if args.Num < len(sc.configs) && args.Num > 0 {
		reply.Config = sc.configs[args.Num].Clone()
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	op := Op{
		Type:     QUERY,
		Num:      args.Num,
		ClientId: args.ClientId,
		MsgId:    args.MsgId,
		Rid:      nrand(),
	}
	e, cf := sc.send(op)
	reply.Err = e
	if cf != nil {
		reply.Config = *cf
	}
}

func (sc *ShardCtrler) rebanlence(conf *Config) {
	if len(conf.Groups) == 0 {
		conf.Shards = [NShards]int{0, 0, 0, 0, 0, 0, 0, 0, 0}
	} else if len(conf.Groups) > NShards {
		gids := make([]int, 0)
		for k := range conf.Groups {
			gids = append(gids, k)
		}
		sort.Ints(gids)
		copy(conf.Shards[:], gids)
		cp := make(map[int][]string)
		for _, s := range conf.Shards {
			if v, ex := conf.Groups[s]; ex {
				cp[s] = v
			}
		}
		conf.Groups = cp
	} else {
		gids := make([]int, 0)
		for k := range conf.Groups {
			gids = append(gids, k)
		}
		sort.Ints(gids)
		i := 0
		for (i+1)*len(gids) < NShards {
			copy(conf.Shards[i*len(gids):(i+1)*len(gids)], gids)
			i++
		}
		copy(conf.Shards[i*len(gids):], gids)
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) existed(cid, mid int64) bool {
	if v, ok := sc.history[cid]; ok && v == mid {
		return true
	}
	return false
}

func (sc *ShardCtrler) handleCommand() {
	for m := range sc.applyCh {
		if m.CommandValid {
			op := m.Command.(Op)
			switch op.Type {
			case JOIN:
				sc.execute(&op, sc.handleJoin, true)
			case LEAVE:
				sc.execute(&op, sc.handleLeave, true)
			case MOVE:
				sc.execute(&op, sc.handleMove, true)
			case QUERY:
				sc.execute(&op, sc.handleQuery, false)
			}
		}
	}
}

func (sc *ShardCtrler) execute(op *Op, h Handler, checkExisted bool) {
	value := ValueWrap{}
	sc.mu.Lock()
	if checkExisted && sc.existed(op.ClientId, op.MsgId) {
		if c, ok := sc.chs[op.Rid]; ok {
			value.Err = ErrDuplicate
			c <- value
		}
		sc.mu.Unlock()
		return
	}
	sc.history[op.ClientId] = op.MsgId
	value.Value = (h)(op)
	if c, ok := sc.chs[op.Rid]; ok {
		value.Err = OK
		c <- value
	}
	sc.mu.Unlock()
}

func (sc *ShardCtrler) handleJoin(op *Op) *Config {
	conf := sc.configs[len(sc.configs)-1].Clone()
	for k, v := range op.Servers {
		if gs, ok := conf.Groups[k]; ok {
			exs := make(map[string]struct{})
			for _, g := range gs {
				exs[g] = struct{}{}
			}
			for _, s := range v {
				if _, existed := exs[s]; !existed {
					conf.Groups[k] = append(conf.Groups[k], s)
				}
			}
		} else {
			conf.Groups[k] = v
		}
	}
	conf.Num = conf.Num + 1
	sc.rebanlence(&conf)
	sc.configs = append(sc.configs, conf)
	return nil
}

func (sc *ShardCtrler) handleLeave(op *Op) *Config {
	conf := sc.configs[len(sc.configs)-1].Clone()
	exists := make(map[int]int)
	for _, gid := range op.GIDs {
		if _, ex := conf.Groups[gid]; ex {
			delete(conf.Groups, gid)
			exists[gid] = 1
		} else {
			continue
		}
	}
	if len(exists) < 1 {
		return nil
	}
	for i := 0; i < len(conf.Shards); i++ {
		if _, ok := exists[conf.Shards[i]]; ok {
			conf.Shards[i] = 0
		}
	}
	conf.Num += 1
	sc.rebanlence(&conf)
	sc.configs = append(sc.configs, conf)
	return nil
}

func (sc *ShardCtrler) handleQuery(op *Op) *Config {
	var conf Config
	if op.Num < 0 || op.Num >= len(sc.configs) {
		conf = sc.configs[len(sc.configs)-1].Clone()
	} else {
		conf = sc.configs[op.Num].Clone()
	}
	return &conf
}

func (sc *ShardCtrler) handleMove(op *Op) *Config {
	conf := sc.configs[len(sc.configs)-1].Clone()
	conf.Shards[op.Shard] = op.GID
	conf.Num = conf.Num + 1
	sc.configs = append(sc.configs, conf)
	return nil
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.chs = make(map[int64]chan ValueWrap)
	sc.history = make(map[int64]int64)
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.

	go sc.handleCommand()
	return sc
}
