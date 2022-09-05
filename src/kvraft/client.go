package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeader int
	clientId   int64
	timeout    time.Duration
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.timeout = time.Millisecond * 10
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		MsgId:    nrand(),
	}

	var ok bool
	for i := ck.lastLeader; i < len(ck.servers); {
		reply := GetReply{}
		ok = ck.servers[i].Call("KVServer.Get", &args, &reply)
		if reply.Err == ErrWrongLeader || !ok {
			i = (i + 1) % len(ck.servers)
			time.Sleep(ck.timeout)
		} else {
			ck.lastLeader = i
		}
		if ok && reply.Err == OK || reply.Err == ErrNoKey {
			return reply.Value
		}
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		MsgId:    nrand(),
	}

	var ok bool
	for i := ck.lastLeader; i < len(ck.servers); {
		reply := PutAppendReply{}
		ok = ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		if reply.Err == ErrWrongLeader || !ok {
			i = (i + 1) % len(ck.servers)
			time.Sleep(ck.timeout)
		} else {
			ck.lastLeader = i
		}
		if ok && reply.Err == OK || reply.Err == ErrDuplicate {
			break
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
