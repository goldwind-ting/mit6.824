package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
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
	// Your code here.
	ck.clientId = nrand()
	ck.timeout = time.Millisecond * 10
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.ClientId = ck.clientId
	args.MsgId = nrand()

	args.Num = num
	var ok bool
	for i := ck.lastLeader; i < len(ck.servers); {
		var reply QueryReply
		ok = ck.servers[i].Call("ShardCtrler.Query", args, &reply)
		if reply.Err == ErrWrongLeader || !ok {
			i = (i + 1) % len(ck.servers)
			time.Sleep(ck.timeout)
			continue
		} else {
			ck.lastLeader = i
		}
		if ok && reply.Err == OK {
			return reply.Config
		}
	}
	return Config{}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.ClientId = ck.clientId
	args.MsgId = nrand()
	args.Servers = servers
	var ok bool
	for i := ck.lastLeader; i < len(ck.servers); {
		var reply JoinReply
		ok = ck.servers[i].Call("ShardCtrler.Join", args, &reply)
		if reply.Err == ErrWrongLeader || !ok {
			i = (i + 1) % len(ck.servers)
			time.Sleep(ck.timeout)
			continue
		} else {
			ck.lastLeader = i
		}
		if ok && (reply.Err == OK || reply.Err == ErrDuplicate) {
			return
		}
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClientId = ck.clientId
	args.MsgId = nrand()
	var ok bool
	for i := ck.lastLeader; i < len(ck.servers); {
		var reply LeaveReply
		ok = ck.servers[i].Call("ShardCtrler.Leave", args, &reply)
		if reply.Err == ErrWrongLeader || !ok {
			i = (i + 1) % len(ck.servers)
			time.Sleep(ck.timeout)
			continue
		} else {
			ck.lastLeader = i
		}
		if ok && (reply.Err == OK || reply.Err == ErrDuplicate) {
			return
		}
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClientId = ck.clientId
	args.MsgId = nrand()

	var ok bool
	for i := ck.lastLeader; i < len(ck.servers); {
		var reply MoveReply
		ok = ck.servers[i].Call("ShardCtrler.Move", args, &reply)
		if reply.Err == ErrWrongLeader || !ok {
			i = (i + 1) % len(ck.servers)
			time.Sleep(ck.timeout)
			continue
		} else {
			ck.lastLeader = i
		}
		if ok && (reply.Err == OK || reply.Err == ErrDuplicate) {
			return
		}
	}
}
