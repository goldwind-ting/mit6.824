package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"context"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

type Role int

// func init() {
// 	f, err := os.OpenFile("log.log", os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
// 	if err != nil {
// 		return
// 	}
// 	log.SetOutput(f)
// }

const (
	FOLLOWERS Role = iota + 1
	CANDIDATE
	LEADER
)

const (
	HEARTBEATINTERVAL = 150
	ELECTTIMEOUT      = 300
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	Term         int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	votedFor    int
	currentTerm int
	role        Role
	logs        []ApplyMsg
	commitIndex int
	majority    int
	leaderId    int
	timeouts    []int
	ac          chan ApplyMsg
	hbticker    *time.Ticker
	eleticker   *time.Ticker
	nextIndex   []int
	matchIndex  []int

	lastSnapshotIndex int
	lastSnapshotTerm  int
	stopCh            chan struct{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	term := rf.currentTerm
	isleader := rf.role == LEADER
	rf.mu.Unlock()
	return term, isleader
}

func (rf *Raft) Persister() *Persister {
	return rf.persister
}

func (rf *Raft) Debug(domain string, vs ...interface{}) {
	log.Printf("[%s] me: %d, currentTerm: %d, votedFor: %d role: %d, leaderId: %d, commitIndex: %d, lastSnapshotIndex: %d,lastSnapshotTerm: %d, extra: %v\n", domain, rf.me, rf.currentTerm, rf.votedFor, rf.role, rf.leaderId, rf.commitIndex, rf.lastSnapshotIndex, rf.lastSnapshotTerm, vs)
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastSnapshotIndex)
	e.Encode(rf.lastSnapshotTerm)
	e.Encode(rf.commitIndex)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var (
		currentTerm       int
		votedFor          int
		logs              []ApplyMsg
		lastSnapshotIndex int
		lastSnapshotTerm  int
		commitIndex       int
	)

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil || d.Decode(&logs) != nil || d.Decode(&lastSnapshotIndex) != nil ||
		d.Decode(&lastSnapshotTerm) != nil || d.Decode(&commitIndex) != nil {
		log.Panic("failed to read persist")
	} else {
		rf.votedFor = votedFor
		rf.currentTerm = currentTerm
		rf.logs = logs
		rf.lastSnapshotIndex = lastSnapshotIndex
		rf.lastSnapshotTerm = lastSnapshotTerm
		rf.commitIndex = commitIndex
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

func (rf *Raft) InstallSnapshot(args *RequestInstallSnapshotArgs, reply *int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		*reply = rf.currentTerm
		return
	}
	if rf.currentTerm < args.Term {
		if rf.role == LEADER {
			rf.role = FOLLOWERS
		}
		rf.currentTerm = args.Term
	}
	if rf.role == CANDIDATE {
		rf.role = FOLLOWERS
	}
	rf.persist()
	if args.LastIncludeIndex <= rf.lastSnapshotIndex {
		return
	}

	if args.LastIncludeIndex >= rf.logs[len(rf.logs)-1].CommandIndex {
		rf.logs = append([]ApplyMsg(nil), ApplyMsg{CommandValid: true, CommandIndex: args.LastIncludeIndex, Term: args.LastIncludeTerm})
	} else {
		ix := -1
		for i := 1; i < len(rf.logs); i++ {
			if rf.logs[i].Term == args.LastIncludeTerm && rf.logs[i].CommandIndex == args.LastIncludeIndex {
				ix = i
				break
			}
		}
		if ix > 0 {
			rf.logs = append([]ApplyMsg(nil), rf.logs[ix:]...)
		} else {
			rf.logs = append([]ApplyMsg(nil), ApplyMsg{CommandValid: true, CommandIndex: args.LastIncludeIndex, Term: args.LastIncludeTerm})
		}
	}
	rf.commitIndex = max(args.LastIncludeIndex, rf.commitIndex)
	rf.persister.SaveSnapshot(args.Data)
	rf.lastSnapshotIndex = args.LastIncludeIndex
	rf.lastSnapshotTerm = args.LastIncludeTerm
	select {
	case rf.ac <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludeTerm,
		SnapshotIndex: args.LastIncludeIndex,
	}:
	case <-rf.stopCh:
	}
	rf.resetTicker()
	rf.persist()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	go func() {
		rf.mu.Lock()
		if index <= rf.lastSnapshotIndex || index > rf.commitIndex {
			rf.mu.Unlock()
			return
		}
		offsetIndex := index - rf.lastSnapshotIndex
		rf.lastSnapshotTerm = rf.logs[offsetIndex].Term
		rf.lastSnapshotIndex = index
		rf.logs = append([]ApplyMsg(nil), rf.logs[offsetIndex:]...)
		rf.persist()
		rf.persister.SaveSnapshot(snapshot)
		rf.mu.Unlock()
	}()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CandidateId  int
	Term         int
	LastLogIndex int
	LastLogTerm  int
}

func (req *RequestVoteArgs) Copy() labrpc.ArgCopy {
	return req
}

type RequestInstallSnapshotArgs struct {
	LeaderId         int
	Term             int
	LastIncludeIndex int
	LastIncludeTerm  int
	Data             []byte
}

func (req *RequestInstallSnapshotArgs) Copy() labrpc.ArgCopy {
	return req
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	} else if args.Term == rf.currentTerm {
		if rf.role == LEADER {
			reply.VoteGranted = false
			return
		}
		if rf.votedFor >= 0 && rf.votedFor != args.CandidateId {
			reply.VoteGranted = false
			return
		}
	}
	if args.Term > rf.currentTerm {
		if rf.role == CANDIDATE || rf.role == LEADER {
			rf.role = FOLLOWERS
		}
		rf.currentTerm = args.Term
		rf.persist()
	}
	// update term before checking last log info
	if rf.logs[len(rf.logs)-1].Term > args.LastLogTerm || (rf.logs[len(rf.logs)-1].Term == args.LastLogTerm && rf.logs[len(rf.logs)-1].CommandIndex > args.LastLogIndex) {
		reply.VoteGranted = false
		return
	}
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	rf.persist()
	rf.resetTicker()
}

type RequestAppendEntryArgs struct {
	LeaderId     int
	Term         int
	Entrys       []ApplyMsg
	CommitIndex  int
	PrevLogTerm  int
	PrevLogIndex int
}

func (req *RequestAppendEntryArgs) Copy() labrpc.ArgCopy {
	ce := make([]ApplyMsg, len(req.Entrys))
	copy(ce, req.Entrys)
	c := *req
	c.Entrys = ce
	return &c
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestAppendEntryReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *RequestAppendEntryArgs, reply *RequestAppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		return
	}
	rf.resetTicker()
	if rf.currentTerm < args.Term {
		if rf.role == LEADER {
			rf.role = FOLLOWERS
		}
		rf.currentTerm = args.Term
	}
	if rf.role == CANDIDATE {
		rf.role = FOLLOWERS
	}
	if rf.role != LEADER && rf.leaderId != args.LeaderId {
		rf.leaderId = args.LeaderId
	}
	rf.persist()
	if args.PrevLogIndex > rf.logs[len(rf.logs)-1].CommandIndex {
		reply.ConflictIndex = max(1, rf.logs[len(rf.logs)-1].CommandIndex+1)
		reply.ConflictTerm = -1
		return
	} else if args.PrevLogIndex-rf.lastSnapshotIndex < 0 {
		reply.ConflictIndex = rf.lastSnapshotIndex + 1
		reply.ConflictTerm = -1
		return
	} else if rf.logs[args.PrevLogIndex-rf.lastSnapshotIndex].Term != args.PrevLogTerm {
		nix := args.PrevLogIndex - rf.lastSnapshotIndex
		reply.ConflictTerm = rf.logs[nix].Term
		for i := nix - 1; i >= 0; i-- {
			if rf.logs[i].Term != rf.logs[nix].Term {
				reply.ConflictIndex = rf.logs[i+1].CommandIndex
				break
			}
		}
		return
	}
	i := 0
	for ; i < len(args.Entrys); i++ {
		if args.PrevLogIndex+1+i > rf.logs[len(rf.logs)-1].CommandIndex {
			break
		}
		if rf.logs[args.PrevLogIndex+1+i-rf.lastSnapshotIndex].Term != args.Entrys[i].Term {
			rf.logs = rf.logs[:args.PrevLogIndex+i+1-rf.lastSnapshotIndex]
			break
		}
	}
	rf.logs = append(rf.logs, args.Entrys[i:]...)
	oldIndex := max(rf.commitIndex, rf.lastSnapshotIndex)
	rf.commitIndex = max(rf.lastSnapshotIndex, min(args.CommitIndex, len(rf.logs)-1+rf.lastSnapshotIndex))
	if oldIndex < rf.commitIndex {
	Send:
		for i := oldIndex + 1; i < rf.commitIndex+1; i++ {
			select {
			case rf.ac <- rf.logs[i-rf.lastSnapshotIndex]:
			case <-rf.stopCh:
				break Send
			}
		}
	}
	rf.persist()
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *RequestAppendEntryArgs, reply *RequestAppendEntryReply, sub bool) bool {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
	defer cancel()
	ch := make(chan bool, 1)
	go func(c chan bool) {
		times := 0
		ok := false
		for times < 2 && !ok {
			ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
			time.Sleep(time.Millisecond * 10)
			times += 1
		}
		ch <- ok
	}(ch)
	select {
	case ok := <-ch:
		if ok {
			rf.mu.Lock()
			if reply.Success {
				rf.matchIndex[server] = args.PrevLogIndex + len(args.Entrys)
				rf.nextIndex[server] = rf.matchIndex[server] + 1
				if rf.matchIndex[server] > rf.commitIndex {
					rf.syncCommitIndex(server)
				}
			} else if reply.Term <= rf.currentTerm && sub {
				if reply.ConflictTerm == -1 {
					rf.nextIndex[server] = max(reply.ConflictIndex, 1)
				} else {
					rf.nextIndex[server] = max(1, reply.ConflictIndex)
					ix := -1
					for i := len(rf.logs) - 1; i >= 1; i-- {
						if rf.logs[i].Term == reply.ConflictTerm {
							ix = i
							break
						}
					}
					if ix >= 0 {
						rf.nextIndex[server] = ix + 1 + rf.lastSnapshotIndex
					}
				}
			} else if reply.Term > rf.currentTerm {
				rf.role = FOLLOWERS
				rf.currentTerm = reply.Term
				rf.persist()
			}
			rf.mu.Unlock()
		}
		return ok
	case <-ctx.Done():
		return false
	case <-rf.stopCh:
		return false
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, wg *sync.WaitGroup) bool {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
	defer cancel()
	defer wg.Done()
	ch := make(chan bool, 1)
	go func(c chan bool) {
		times := 0
		ok := false
		for times < 2 && !ok {
			ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
			time.Sleep(time.Millisecond * 10)
			times += 1
		}
		ch <- ok
	}(ch)
	select {
	case ok := <-ch:
		return ok
	case <-ctx.Done():
		return false
	case <-rf.stopCh:
		return false
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *RequestInstallSnapshotArgs, reply *int) bool {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
	defer cancel()
	ch := make(chan bool, 1)
	go func(c chan bool) {
		times := 0
		ok := false
		for times < 2 && !ok {
			ok = rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
			time.Sleep(time.Millisecond * 10)
			times += 1
		}
		ch <- ok
	}(ch)
	select {
	case ok := <-ch:
		if ok {
			if *reply > 0 {
				rf.mu.Lock()
				rf.role = FOLLOWERS
				rf.currentTerm = *reply
				rf.persist()
				rf.mu.Unlock()
			} else {
				rf.mu.Lock()
				rf.nextIndex[server] = args.LastIncludeIndex + 1
				rf.matchIndex[server] = args.LastIncludeIndex
				rf.mu.Unlock()
			}
		}
		return ok
	case <-ctx.Done():
		return false
	case <-rf.stopCh:
		return false
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	rf.mu.Lock()
	if rf.role != LEADER {
		rf.mu.Unlock()
		return index, term, false
	}
	term = rf.currentTerm
	index = len(rf.logs) + rf.lastSnapshotIndex
	args := RequestAppendEntryArgs{
		LeaderId: rf.me,
		Term:     term,
		Entrys: []ApplyMsg{
			{
				CommandValid:  true,
				Command:       command,
				Term:          term,
				CommandIndex:  index,
				SnapshotValid: false,
			},
		},
		CommitIndex:  rf.commitIndex,
		PrevLogIndex: len(rf.logs) - 1 + rf.lastSnapshotIndex,
		PrevLogTerm:  rf.logs[len(rf.logs)-1].Term,
	}
	rf.logs = append(rf.logs, args.Entrys...)
	rf.persist()
	rf.mu.Unlock()
	replys := make([]RequestAppendEntryReply, len(rf.peers))
	for p := range rf.peers {
		if p == rf.me {
			continue
		}
		go rf.sendAppendEntries(p, &args, &replys[p], false)
	}
	return index, term, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	close(rf.stopCh)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) initializeIndex() {
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = max(len(rf.logs)+rf.lastSnapshotIndex, 1+rf.lastSnapshotIndex)
		rf.matchIndex[i] = rf.lastSnapshotIndex
	}
}

func (rf *Raft) heartbeatTicker() {
	for !rf.killed() {
		<-rf.hbticker.C
		replys := make([]RequestAppendEntryReply, len(rf.peers))
		for p := range rf.peers {
			rf.mu.Lock()
			if p == rf.me {
				rf.mu.Unlock()
				continue
			}
			if rf.role != LEADER {
				rf.mu.Unlock()
				return
			}
			if rf.nextIndex[p] <= rf.lastSnapshotIndex {
				term := 0
				go rf.sendInstallSnapshot(p, &RequestInstallSnapshotArgs{
					LeaderId:         rf.me,
					Term:             rf.currentTerm,
					LastIncludeIndex: rf.lastSnapshotIndex,
					LastIncludeTerm:  rf.lastSnapshotTerm,
					Data:             rf.persister.ReadSnapshot(),
				}, &term)
				rf.mu.Unlock()
				continue
			}
			var entrys []ApplyMsg
			if len(rf.logs)+rf.lastSnapshotIndex > rf.nextIndex[p] {
				entrys = rf.logs[rf.nextIndex[p]-rf.lastSnapshotIndex:]
			}
			go rf.sendAppendEntries(p, &RequestAppendEntryArgs{
				LeaderId:     rf.me,
				Term:         rf.currentTerm,
				Entrys:       entrys,
				PrevLogTerm:  rf.logs[rf.nextIndex[p]-1-rf.lastSnapshotIndex].Term,
				PrevLogIndex: rf.nextIndex[p] - 1,
				CommitIndex:  rf.commitIndex,
			}, &replys[p], true)
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) syncCommitIndex(server int) {
	for i := rf.commitIndex + 1; i <= rf.matchIndex[server]; i++ {
		count := 0
		for p := range rf.peers {
			if p == rf.me || rf.matchIndex[p] >= i {
				count++
			}
		}
		if count >= rf.majority {
			rf.commitIndex++
			select {
			case rf.ac <- rf.logs[rf.commitIndex-rf.lastSnapshotIndex]:
			case <-rf.stopCh:
				return
			}
		}
	}
}

func (rf *Raft) resetTicker() {
	rand.Seed(time.Now().UnixNano())
	to := time.Duration(rf.timeouts[rand.Intn(len(rf.peers))]+200) * time.Millisecond
	rf.eleticker.Reset(to)
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	rf.resetTicker()
	<-rf.eleticker.C
	for !rf.killed() {
		rf.mu.Lock()
		role := rf.role
		rf.mu.Unlock()
		if role == LEADER {
			time.Sleep(time.Millisecond * 50)
			continue
		}
		go rf.elect()
		<-rf.eleticker.C
	}
}

func (rf *Raft) elect() {
	rf.mu.Lock()
	rf.currentTerm += 1
	rf.role = CANDIDATE
	rf.votedFor = rf.me
	rf.persist()
	rf.mu.Unlock()
	votes := 1
	var wg sync.WaitGroup
	replys := make([]RequestVoteReply, len(rf.peers))
	wg.Add(len(rf.peers) - 1)
	lastLogTerm := -1
	lastLogIndex := -1
	rf.mu.Lock()
	if len(rf.logs) > 0 {
		lastLogTerm = rf.logs[len(rf.logs)-1].Term
		lastLogIndex = rf.logs[len(rf.logs)-1].CommandIndex
	}
	me := rf.me
	term := rf.currentTerm
	rf.mu.Unlock()
	for p := range rf.peers {
		if me == p {
			continue
		}
		args := RequestVoteArgs{
			CandidateId:  me,
			Term:         term,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}
		go rf.sendRequestVote(p, &args, &replys[p], &wg)
	}
	wg.Wait()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for _, rep := range replys {
		if rep.VoteGranted && term == rf.currentTerm {
			votes += 1
		}
		if rep.Term > rf.currentTerm {
			rf.currentTerm = rep.Term
			rf.role = FOLLOWERS
		}
	}
	if votes >= rf.majority && rf.role == CANDIDATE {
		rf.role = LEADER
		rf.initializeIndex()
		go rf.heartbeatTicker()
	}
	rf.persist()
	rf.resetTicker()
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.role = FOLLOWERS
	rf.leaderId = -1
	rand.Seed(time.Now().UnixNano())
	rf.logs = make([]ApplyMsg, 0)
	rf.logs = append(rf.logs, ApplyMsg{
		CommandIndex: 0,
		Term:         0,
	})
	rf.ac = applyCh
	rf.stopCh = make(chan struct{}, 1)
	rf.hbticker = time.NewTicker(time.Millisecond * HEARTBEATINTERVAL)
	rf.eleticker = time.NewTicker(time.Millisecond * ELECTTIMEOUT)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.readPersist(persister.ReadRaftState())
	rf.initializeIndex()
	if len(rf.peers)%2 == 0 {
		rf.majority = len(rf.peers)/2 + 1
	} else {
		rf.majority = (len(rf.peers) + 1) / 2
	}
	for i := 0; i < len(rf.peers); i++ {
		rf.timeouts = append(rf.timeouts, 10*i+1)
	}
	rf.commitIndex = rf.lastSnapshotIndex
	go rf.ticker()

	return rf
}
