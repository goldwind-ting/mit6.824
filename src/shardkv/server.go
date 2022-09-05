package shardkv

import (
	"bytes"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

const ISDEBUG = false

func init() {
	f, err := os.OpenFile("log.log", os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
	if err != nil {
		return
	}
	log.SetOutput(f)
}

type ValueWrap struct {
	Value string
	Err   Err
	Data  map[int]map[string]string
}

type HistoryDB map[int]map[string]string

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type      OpType
	Rid       int64
	ClientId  int64
	MsgId     int64
	Key       string
	Value     string
	Shard     int
	Shards    map[int]struct{}
	Status    Status
	Data      map[int]map[string]string
	ConfigNum int
	Conf      shardctrler.Config
	History   map[int]map[int64]int64
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	Config       *shardctrler.Config

	// Your definitions here.
	sc              *shardctrler.Clerk
	db              map[int]map[string]string
	chs             map[int64]chan ValueWrap
	groupHistory    map[int]map[int64]int64
	tmpGroupHistory map[int]map[int]map[int64]int64
	selfHistory     map[int64]int64
	lastApply       int
	status          Status
	stopCh          chan struct{}
	id              int64
	tmpDB           map[int]HistoryDB
	lastMigration   map[int]struct{}
	shardConfigNum  map[int]int
}

func (kv *ShardKV) Debug(domain string, vs ...interface{}) {
	if ISDEBUG {
		log.Printf("[%s], gid: %d, me: %d, lastApply: %d, status: %d, extra: %v\n", domain, kv.gid, kv.me, kv.lastApply, kv.status, vs)
	}
}

func (kv *ShardKV) groupHistoryExisted(shard int, cid, mid int64) bool {
	h, sex := kv.groupHistory[shard]
	if sex {
		if v, ok := h[cid]; ok && v == mid {
			return true
		}
	}
	return false
}

func (kv *ShardKV) selfHistoryExisted(cid, mid int64) bool {
	if v, ok := kv.selfHistory[cid]; ok && v == mid {
		return true
	}
	return false
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	if kv.status == LEAVE {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}
	kv.mu.Unlock()
	op := Op{
		Type:     GET,
		ClientId: args.ClientId,
		MsgId:    args.MsgId,
		Rid:      nrand(),
		Key:      args.Key,
		Shard:    args.Shard,
	}
	e, v, _ := kv.send(op)
	reply.Err = e
	reply.Value = v
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	if kv.status == LEAVE {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}
	kv.mu.Unlock()
	var op Op
	if args.Op == "Put" {
		op = Op{
			Type:     PUT,
			ClientId: args.ClientId,
			MsgId:    args.MsgId,
			Rid:      nrand(),
			Key:      args.Key,
			Value:    args.Value,
			Shard:    args.Shard,
		}
	} else if args.Op == "Append" {
		op = Op{
			Type:      APPEND,
			ClientId:  args.ClientId,
			MsgId:     args.MsgId,
			Rid:       nrand(),
			Key:       args.Key,
			Value:     args.Value,
			Shard:     args.Shard,
			ConfigNum: kv.Config.Num,
		}
	}
	e, _, _ := kv.send(op)
	reply.Err = e
}

func (kv *ShardKV) send(op Op) (e Err, value string, data map[int]map[string]string) {
	c := make(chan ValueWrap, 1)
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		e = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	kv.chs[op.Rid] = c
	kv.mu.Unlock()
	select {
	case <-time.After(time.Millisecond * 500):
		e = ErrTimeout
	case v := <-c:
		e = v.Err
		value = v.Value
		data = v.Data
	}
	kv.mu.Lock()
	delete(kv.chs, op.Rid)
	kv.mu.Unlock()
	return
}

func (kv *ShardKV) exceedThreshold() bool {
	return kv.maxraftstate > 0 && kv.rf.Persister().RaftStateSize() > kv.maxraftstate
}

func (kv *ShardKV) makeSnapshot() {
	if kv.exceedThreshold() {
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		kv.mu.Lock()
		e.Encode(kv.db)
		e.Encode(kv.groupHistory)
		e.Encode(kv.selfHistory)
		e.Encode(kv.Config)
		e.Encode(kv.status)
		e.Encode(kv.tmpDB)
		e.Encode(kv.lastMigration)
		e.Encode(kv.tmpGroupHistory)
		e.Encode(kv.shardConfigNum)
		b := w.Bytes()
		kv.rf.Snapshot(kv.lastApply, b)
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) handleCommand(op Op, ix int) {
	switch op.Type {
	case GET:
		kv.handleGet(op, ix)
	case PUT:
		kv.handlePut(op, ix)
	case APPEND:
		kv.handleAppend(op, ix)
	case SYNCDB:
		kv.handleSyncDB(op, ix)
	case SYNCSTATUS:
		kv.handleSyncStatus(op, ix)
	}
}

func (kv *ShardKV) handleGet(op Op, ix int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.lastApply = ix
	if c, ok := kv.chs[op.Rid]; ok {
		if kv.Config.Shards[op.Shard] != kv.gid || kv.status == LEAVE {
			c <- ValueWrap{
				Value: "",
				Err:   ErrWrongGroup,
			}
			return
		}
		if m, existed := kv.db[op.Shard]; !existed {
			if kv.status == ONLINE {
				panic("unreached")
			}
			c <- ValueWrap{
				Value: "",
				Err:   ErrMigration,
			}
		} else if value, ok := m[op.Key]; ok {
			c <- ValueWrap{
				Value: value,
				Err:   OK,
			}
		} else if kv.status != ONLINE {
			c <- ValueWrap{
				Value: "",
				Err:   ErrMigration,
			}
		} else {
			c <- ValueWrap{
				Value: "",
				Err:   ErrNoKey,
			}
		}
	}
}

func (kv *ShardKV) handlePut(op Op, ix int) {
	value := ValueWrap{}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.lastApply = ix
	if kv.groupHistoryExisted(op.Shard, op.ClientId, op.MsgId) {
		value.Err = ErrDuplicate
		if c, ok := kv.chs[op.Rid]; ok {
			c <- value
		}
	} else {
		value.Err = OK
		c, ok := kv.chs[op.Rid]
		if kv.status != LEAVE && kv.Config.Shards[op.Shard] == kv.gid {
			if _, existed := kv.db[op.Shard]; !existed {
				kv.db[op.Shard] = make(map[string]string)
				kv.groupHistory[op.Shard] = make(map[int64]int64)
				kv.shardConfigNum[op.Shard] = kv.Config.Num
			}
			kv.groupHistory[op.Shard][op.ClientId] = op.MsgId
			kv.db[op.Shard][op.Key] = op.Value
			if ok {
				c <- value
			}
		} else if ok {
			value.Err = ErrWrongGroup
			c <- value
		}
	}
}

func (kv *ShardKV) handleAppend(op Op, ix int) {
	value := ValueWrap{}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.lastApply = ix
	if kv.groupHistoryExisted(op.Shard, op.ClientId, op.MsgId) {
		value.Err = ErrDuplicate
		if c, ok := kv.chs[op.Rid]; ok {
			c <- value
		}
	} else {
		value.Err = OK
		c, rok := kv.chs[op.Rid]
		if kv.Config.Shards[op.Shard] == kv.gid && kv.status != LEAVE {
			if m, existed := kv.db[op.Shard]; !existed {
				value.Err = ErrMigration
			} else if v, ok := m[op.Key]; ok && kv.status == ONLINE {
				kv.groupHistory[op.Shard][op.ClientId] = op.MsgId
				kv.db[op.Shard][op.Key] = strings.Join([]string{v, op.Value}, "")
			} else if kv.status != ONLINE {
				value.Err = ErrMigration
			} else {
				value.Err = ErrNoKey
			}
			if rok {
				c <- value
			}
		} else {
			value.Err = ErrWrongGroup
			if rok {
				c <- value
			}
		}
	}
}

func (kv *ShardKV) Listen() {
	for {
		select {
		case m := <-kv.applyCh:
			if !m.CommandValid && m.SnapshotValid {
				kv.mu.Lock()
				r := bytes.NewBuffer(kv.rf.Persister().ReadSnapshot())
				d := labgob.NewDecoder(r)
				kv.status = LEAVE
				d.Decode(&kv.db)
				d.Decode(&kv.groupHistory)
				d.Decode(&kv.selfHistory)
				d.Decode(&kv.Config)
				d.Decode(&kv.status)
				d.Decode(&kv.tmpDB)
				d.Decode(&kv.lastMigration)
				d.Decode(&kv.tmpGroupHistory)
				d.Decode(&kv.shardConfigNum)
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

func (kv *ShardKV) syncConfiguration() {
	for {
		_, isLeader := kv.rf.GetState()
		if isLeader {
			var (
				conf      shardctrler.Config
				newStatus Status
			)
			if kv.Config == nil {
				conf = kv.sc.Query(1)
			} else {
				conf = kv.sc.Query(kv.Config.Num + 1)
			}

			kv.mu.Lock()
			if kv.Config != nil && (conf.Num <= kv.Config.Num || !kv.migrated(kv.Config.Num)) {
				kv.mu.Unlock()
				time.Sleep(time.Millisecond * 100)
				continue
			}
			_, ok2 := conf.Groups[kv.gid]
			if kv.Config == nil {
				if ok2 && len(conf.Groups) > 1 {
					newStatus = PULL
				} else if ok2 {
					newStatus = ONLINE
				} else {
					newStatus = LEAVE
				}
			} else {
				_, ok1 := kv.Config.Groups[kv.gid]
				if ok1 && !ok2 {
					newStatus = LEAVE
				} else if !ok1 && ok2 && len(kv.Config.Groups) > 0 {
					newStatus = PULL
				} else if ok1 && ok2 {
					newStatus = PULL
				} else if !ok1 && ok2 && len(kv.Config.Groups) == 0 {
					newStatus = ONLINE
				}
			}
			kv.mu.Unlock()
			op := Op{
				Type:     SYNCSTATUS,
				ClientId: kv.id,
				MsgId:    nrand(),
				Rid:      nrand(),
				Status:   newStatus,
				Conf:     conf,
			}
			for {
				e, _, _ := kv.send(op)
				if e == ErrWrongLeader || e == ErrWrongGroup {
					panic(e)
				}
				if e == OK || e == ErrDuplicate {
					break
				}
				time.Sleep(time.Millisecond * 50)
			}
		}
		select {
		case <-time.After(time.Millisecond * 100):
		case <-kv.stopCh:
			return
		}
	}
}

func (kv *ShardKV) listenStatus() {
	for {
		_, isLeader := kv.rf.GetState()
		if isLeader {
			kv.mu.Lock()
			if kv.Config == nil || !kv.migrated(kv.Config.Num) {
				if kv.status == PULL {
					old := kv.sc.Query(kv.Config.Num - 1)
					go kv.pullMigration(&old, kv.Config)
				}
			}
			kv.mu.Unlock()
		}
		select {
		case <-time.After(time.Millisecond * 100):
		case <-kv.stopCh:
			return
		}
	}
}

func (kv *ShardKV) Migration(args *MigrationArgs, reply *MigrationReply) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Err = OK
	reply.Data = make(map[int]map[string]string)
	reply.History = make(map[int]map[int64]int64)
	kv.mu.Lock()
	db, ex := kv.tmpDB[args.ConfigNum]
	// TODO: optimize tmpDB
	kv.mu.Unlock()
	if ex {
		for _, s := range args.Shards {
			if v, ex := db[s]; ex {
				reply.Data[s] = v
				reply.History[s] = kv.tmpGroupHistory[args.ConfigNum][s]
			}
		}

	} else {
		reply.Err = ErrMigration
	}
}

func (kv *ShardKV) pullMigration(old, new *shardctrler.Config) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		return
	}
	ss := make([]int, 0)
	for ix, s := range new.Shards {
		if s == kv.gid {
			ss = append(ss, ix)
		}
	}
	gids := make(map[int][]int)
	totalShards := make(map[int]struct{}, 0)
	for _, s := range ss {
		if old.Shards[s] != kv.gid {
			gids[old.Shards[s]] = append(gids[old.Shards[s]], s)
			totalShards[s] = struct{}{}
		}
	}
	mid := nrand()
	kv.mu.Lock()
	for gid, shards := range gids {
		go func(gid int, shards []int) {
			args := MigrationArgs{
				Shards:    shards,
				MsgId:     mid,
				ClientId:  kv.id,
				ConfigNum: new.Num,
			}
			var ok bool
			for i := 0; i < len(old.Groups[gid]); {
				reply := MigrationReply{}
				ok = kv.make_end(old.Groups[gid][i]).Call("ShardKV.Migration", &args, &reply)
				if reply.Err == ErrWrongLeader || !ok {
					i = (i + 1) % len(old.Groups[gid])
					time.Sleep(time.Millisecond * 100)
				}
				if ok && (reply.Err == OK || reply.Err == ErrDuplicate) {
					if reply.Err == OK {
						kv.syncDB(reply.Data, new.Num, totalShards, reply.History)
					}
					break
				} else if ok && reply.Err == ErrMigration {
					time.Sleep(time.Millisecond * 100)
				}
			}
		}(gid, shards)
	}
	kv.mu.Unlock()
	if len(gids) < 1 {
		kv.syncDB(nil, new.Num, nil, nil)
	}
}

func (kv *ShardKV) SyncDB(args *SyncDBArgs, reply *SyncDBReply) {
	var op = Op{
		Type:      SYNCDB,
		ClientId:  kv.id,
		MsgId:     nrand(),
		Rid:       nrand(),
		ConfigNum: args.ConfigNum,
		Data:      args.Data,
		Shards:    args.Shards,
		History:   args.History,
	}
	reply.Err, _, _ = kv.send(op)
}

func (kv *ShardKV) syncDB(db map[int]map[string]string, cn int, shards map[int]struct{}, history map[int]map[int64]int64) {
	args := SyncDBArgs{
		MsgId:     nrand(),
		ClientId:  kv.id,
		Shards:    shards,
		ConfigNum: cn,
		Data:      db,
		History:   history,
	}
	if servers, ok := kv.Config.Groups[kv.gid]; ok {
		for i := kv.me; i < len(servers); {
			var reply SyncDBReply
			ok = kv.make_end(servers[i]).Call("ShardKV.SyncDB", &args, &reply)
			if !ok || reply.Err == ErrWrongLeader {
				i = (i + 1) % len(servers)
				time.Sleep(time.Millisecond * 50)
				continue
			}
			if ok && (reply.Err == OK || reply.Err == ErrDuplicate) {
				return
			}
			if ok && reply.Err == ErrWrongGroup {
				panic(reply.Err)
			}
		}
	}
}

func (kv *ShardKV) migrated(cn int) bool {
	_, migrated := kv.lastMigration[cn]
	return migrated || kv.status == LEAVE
}

func (kv *ShardKV) handleSyncDB(op Op, ix int) {
	value := ValueWrap{}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.lastApply = ix
	if kv.selfHistoryExisted(op.ClientId, op.MsgId) || kv.migrated(op.ConfigNum) {
		value.Err = ErrDuplicate
		if c, ok := kv.chs[op.Rid]; ok {
			c <- value
		}
	} else {
		value.Err = OK
		kv.selfHistory[op.ClientId] = op.MsgId
		if len(op.Data) > 0 {
			for kd, vd := range op.Data {
				cpvd := make(map[string]string)
				cpvh := make(map[int64]int64)
				for kk, vv := range vd {
					cpvd[kk] = vv
				}
				for kkh, vvh := range op.History[kd] {
					cpvh[kkh] = vvh
				}
				cn := kv.shardConfigNum[kd]
				if m, ex := kv.db[kd]; ex {
					if cn >= kv.Config.Num {
						for kkd, vvd := range m {
							cpvd[kkd] = vvd
						}
						for kkh, vvh := range kv.groupHistory[kd] {
							cpvh[kkh] = vvh
						}
					} else {
						kv.shardConfigNum[kd] = kv.Config.Num
					}
					kv.db[kd] = cpvd
					kv.groupHistory[kd] = cpvh
				} else {
					kv.db[kd] = cpvd
					kv.groupHistory[kd] = cpvh
					kv.shardConfigNum[kd] = kv.Config.Num
				}
			}
		} else if len(kv.db) < 1 {
			for ix, s := range kv.Config.Shards {
				if s == kv.gid {
					kv.db[ix] = make(map[string]string)
					kv.groupHistory[ix] = make(map[int64]int64)
					kv.shardConfigNum[ix] = kv.Config.Num
				}
			}
		}
		count, allExist := 0, true
		for ix, s := range kv.Config.Shards {
			if s == kv.gid {
				count++
			}
			_, sex := op.Shards[ix]
			if _, ex := kv.db[ix]; !ex && s == kv.gid {
				allExist = false
				break
			} else if ex && s != kv.gid {
				delete(kv.db, ix)
			} else if sex && ex && kv.shardConfigNum[ix] < kv.Config.Num {
				allExist = false
				delete(kv.db, ix)
			}
		}
		if len(kv.db) == count && allExist {
			kv.status = ONLINE
			kv.lastMigration[op.ConfigNum] = struct{}{}
		}
		if c, ok := kv.chs[op.Rid]; ok {
			c <- value
		}
	}
}

func (kv *ShardKV) handleSyncStatus(op Op, ix int) {
	value := ValueWrap{}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.lastApply = ix
	if kv.selfHistoryExisted(op.ClientId, op.MsgId) || kv.Config != nil && op.Conf.Num <= kv.Config.Num {
		value.Err = ErrDuplicate
		if c, ok := kv.chs[op.Rid]; ok {
			c <- value
		}
	} else {
		value.Err = OK
		kv.selfHistory[op.ClientId] = op.MsgId
		switch op.Status {
		case ONLINE:
			if kv.Config == nil || kv.Config.Shards != op.Conf.Shards {
				for ix, s := range op.Conf.Shards {
					if s == kv.gid {
						kv.db[ix] = map[string]string{}
						kv.groupHistory[ix] = make(map[int64]int64)
					}
				}
			}
			kv.lastMigration[op.Conf.Num] = struct{}{}
		case LEAVE:
			kv.tmpDB[op.Conf.Num] = kv.db
			kv.tmpGroupHistory[op.Conf.Num] = kv.groupHistory
			kv.db = make(map[int]map[string]string)
			kv.groupHistory = make(map[int]map[int64]int64)
			kv.lastMigration[op.Conf.Num] = struct{}{}
		case PULL:
			if kv.Config != nil && kv.status == LEAVE {
				if _, ex := kv.tmpDB[kv.Config.Num]; ex && len(kv.db) > 0 {
					kv.db = make(map[int]map[string]string)
				} else if !ex && len(kv.db) > 0 {
					kv.tmpDB[kv.Config.Num] = kv.db
					kv.db = make(map[int]map[string]string)
				}
			}
			if _, ex := kv.tmpDB[op.Conf.Num]; !ex {
				kv.tmpDB[op.Conf.Num] = make(HistoryDB)
				kv.tmpGroupHistory[op.Conf.Num] = make(map[int]map[int64]int64)
				for k, v := range kv.db {
					if op.Conf.Shards[k] != kv.gid {
						kv.tmpDB[op.Conf.Num][k] = v
						delete(kv.db, k)
					}
				}
				for k, v := range kv.groupHistory {
					if op.Conf.Shards[k] != kv.gid {
						kv.tmpGroupHistory[op.Conf.Num][k] = v
						delete(kv.groupHistory, k)
					}
				}
			}
		}
		kv.Config = &op.Conf
		kv.status = op.Status
		if c, ok := kv.chs[op.Rid]; ok {
			c <- value
		}
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	close(kv.stopCh)
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.sc = shardctrler.MakeClerk(kv.ctrlers)
	kv.db = make(map[int]map[string]string)
	kv.tmpDB = make(map[int]HistoryDB)
	kv.id = nrand()
	kv.chs = make(map[int64]chan ValueWrap)
	kv.selfHistory = make(map[int64]int64)
	kv.groupHistory = make(map[int]map[int64]int64)
	kv.tmpGroupHistory = make(map[int]map[int]map[int64]int64)
	kv.stopCh = make(chan struct{}, 1)
	kv.lastMigration = make(map[int]struct{})
	kv.shardConfigNum = make(map[int]int, 10)
	for i := 0; i < 10; i++ {
		kv.shardConfigNum[i] = 0
	}
	kv.status = LEAVE
	b := persister.ReadSnapshot()
	if len(b) > 0 {
		r := bytes.NewBuffer(b)
		d := labgob.NewDecoder(r)
		d.Decode(&kv.db)
		d.Decode(&kv.groupHistory)
		d.Decode(&kv.selfHistory)
		d.Decode(&kv.Config)
		d.Decode(&kv.status)
		d.Decode(&kv.tmpDB)
		d.Decode(&kv.lastMigration)
		d.Decode(&kv.tmpGroupHistory)
		d.Decode(&kv.shardConfigNum)
	}
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.syncConfiguration()
	go kv.Listen()
	go kv.listenStatus()
	return kv
}
