package shardkv

import "6.824/labrpc"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

type OpType int

const (
	GET OpType = iota
	PUT
	APPEND
	SYNCDB
	SYNCSTATUS
)

type Status int

const (
	LEAVE Status = iota
	PULL
	ONLINE
)

const (
	OK             Err = "OK"
	ErrNoKey       Err = "ErrNoKey"
	ErrWrongGroup  Err = "ErrWrongGroup"
	ErrWrongLeader Err = "ErrWrongLeader"
	ErrTimeout     Err = "ErrTimeout"
	ErrDuplicate   Err = "ErrDuplicate"
	ErrMigration   Err = "ErrMigration"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	MsgId    int64
	Shard    int
}

func (p *PutAppendArgs) Copy() labrpc.ArgCopy {
	return p
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int64
	MsgId    int64
	Shard    int
}

func (p *GetArgs) Copy() labrpc.ArgCopy {
	return p
}

type GetReply struct {
	Err   Err
	Value string
}

type MigrationArgs struct {
	Shards    []int
	MsgId     int64
	ClientId  int64
	ConfigNum int
}

func (p *MigrationArgs) Copy() labrpc.ArgCopy {
	return p
}

type SyncDBReply struct {
	Err Err
}

type SyncDBArgs struct {
	Shards    map[int]struct{}
	Data      map[int]map[string]string
	MsgId     int64
	ClientId  int64
	ConfigNum int
	History   map[int]map[int64]int64
}

func (p *SyncDBArgs) Copy() labrpc.ArgCopy {
	return p
}

type MigrationReply struct {
	Data    map[int]map[string]string
	History map[int]map[int64]int64
	Err     Err
}
