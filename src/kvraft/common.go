package kvraft

import "6.824/labrpc"

const (
	OK             Err = "OK"
	ErrNoKey       Err = "ErrNoKey"
	ErrWrongLeader Err = "ErrWrongLeader"
	ErrTimeout     Err = "ErrTimeout"
	ErrDuplicate   Err = "ErrDuplicate"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	MsgId    int64
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
}

func (g *GetArgs) Copy() labrpc.ArgCopy {
	return g
}

type GetReply struct {
	Err   Err
	Value string
}
