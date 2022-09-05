package shardctrler

import "6.824/labrpc"

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [10]int          // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func (c *Config) Clone() Config {
	cf := *c
	cf.Groups = make(map[int][]string)
	for k, v := range c.Groups {
		cf.Groups[k] = append([]string{}, v...)
	}
	return cf
}

const (
	OK             Err = "OK"
	ErrNoKey       Err = "ErrNoKey"
	ErrWrongLeader Err = "ErrWrongLeader"
	ErrTimeout     Err = "ErrTimeout"
	ErrDuplicate   Err = "ErrDuplicate"
)

type Err string

type JoinArgs struct {
	Servers  map[int][]string // new GID -> servers mappings
	ClientId int64
	MsgId    int64
}

func (p *JoinArgs) Copy() labrpc.ArgCopy {
	return p
}

type JoinReply struct {
	Err Err
}

type LeaveArgs struct {
	GIDs     []int
	ClientId int64
	MsgId    int64
}

func (p *LeaveArgs) Copy() labrpc.ArgCopy {
	return p
}

type LeaveReply struct {
	Err Err
}

type MoveArgs struct {
	Shard    int
	GID      int
	ClientId int64
	MsgId    int64
}

func (p *MoveArgs) Copy() labrpc.ArgCopy {
	return p
}

type MoveReply struct {
	Err Err
}

type QueryArgs struct {
	Num      int // desired config number
	ClientId int64
	MsgId    int64
}

func (p *QueryArgs) Copy() labrpc.ArgCopy {
	return p
}

type QueryReply struct {
	Err    Err
	Config Config
}
