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

import "sync"
import "labrpc"

import "bytes"
import "encoding/gob"
import "time"
import "math/rand"


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type RaftState int
const (
    FOLLOWER RaftState = iota
    LEADER
    CANDIDATE
)

type LogEntry struct {
    Term    int
    Command interface{}
}

const HEARTBEAT_INTERVAL = 100 * time.Millisecond
const CHECK_INTERVAL = 25 * time.Millisecond
const MIN_ELECTION_TIMEOUT = 300 * time.Millisecond
const MAX_ELECTION_TIMEOUT = 600 * time.Millisecond

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
    state             RaftState
    currentTerm       int
    votedFor          int
    logs              []LogEntry
    voteCount         int
    commitIndex       int
    applyCh           chan ApplyMsg
    matchIndex        []int
    nextIndex         []int
    electionLoopDone  chan bool
    heartbeatLoopDone chan bool
    electionTicker    *time.Ticker
    heartbeatTicker   *time.Ticker
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
    term = rf.currentTerm
    isleader = rf.state == LEADER
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
    w := new(bytes.Buffer)
    e := gob.NewEncoder(w)
    e.Encode(rf.currentTerm)
    e.Encode(rf.votedFor)
    e.Encode(rf.logs)
    data := w.Bytes()
    rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
    r := bytes.NewBuffer(data)
    d := gob.NewDecoder(r)
    d.Decode(&rf.currentTerm)
    d.Decode(&rf.votedFor)
    d.Decode(&rf.logs)
}




//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
    Term         int
    CandidateId  int
    LastLogIndex int
    LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
    Term        int
    VoteGranted bool
}

func (rf *Raft) become(state RaftState) {
    rf.state = state
    if state == FOLLOWER {
        rf.votedFor = -1
        rf.stopHeartBeatTicker()
        rf.resetElectionTicker()
    } else if state == CANDIDATE {
        rf.currentTerm++
        rf.votedFor = rf.me
        rf.voteCount = 1
        go rf.startElection()
    } else if state == LEADER {
        rf.matchIndex = make([]int, len(rf.peers))
        rf.nextIndex = make([]int, len(rf.peers))
        for i := range rf.peers {
            rf.matchIndex[i] = -1
            rf.nextIndex[i] = len(rf.logs)
        }
        rf.resetHeartbeatTicker()
        rf.stopElectionTicker()
    }
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
    rf.mu.Lock()
    defer rf.mu.Unlock()
    // 如果候选人任期小于接收者任期，则拒绝投票
    if args.Term < rf.currentTerm {
        reply.Term = rf.currentTerm
        reply.VoteGranted = false
        return
    }
    // 如果候选人任期大于接收者任期，则接收者更新任期，并转换为跟随者
    if args.Term > rf.currentTerm {
        rf.currentTerm = args.Term
        rf.become(FOLLOWER)
    }
    // 如果已经给当前任期的其他候选人投过票，则拒绝投票
    if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
        reply.Term = rf.currentTerm
        reply.VoteGranted = false
        return
    }
    // 如果候选人日志比接收者旧，则拒绝投票
    t1, t2 := rf.lastLogTerm(), args.LastLogTerm
    i1, i2 := rf.lastLogIndex(), args.LastLogIndex
    if t1 > t2 || (t1 == t2 && i1 > i2) {
        reply.Term = rf.currentTerm
        reply.VoteGranted = false
        return
    }
    // 投票给候选人
    rf.votedFor = args.CandidateId
    reply.Term = rf.currentTerm
    reply.VoteGranted = true
}

func (rf* Raft) lastLogIndex() int {
    return len(rf.logs) - 1
}

func (rf* Raft) termAt(index int) int {
    if index < 0 || index >= len(rf.logs) {
        return 0
    } else {
        return rf.logs[index].Term
    }
}

func (rf* Raft) lastLogTerm() int {
    return rf.termAt(rf.lastLogIndex())
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
    if ok && rf.state == CANDIDATE {
        rf.mu.Lock()
        defer rf.mu.Unlock()

        // 如果当前任期小于接收者任期，则更新当前任期，并转换为跟随者
        if rf.currentTerm < reply.Term {
            rf.currentTerm = reply.Term
            rf.become(FOLLOWER)
            return ok
        }

        // 如果获得了投票，则增加投票数
        if reply.VoteGranted {
            rf.voteCount++
            // 如果获得了多数投票，则转换为领导者
            if rf.voteCount > len(rf.peers) / 2 {
                rf.become(LEADER)
            }
        }
    }
	return ok
}

type AppendEntriesArgs struct {
    Term         int
    LeaderId     int
    PrevLogIndex int
    PrevLogTerm  int
    Entries      []LogEntry
    LeaderCommit int
}

type AppendEntriesReply struct {
    Term    int
    Success bool
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    // 如果领导者任期小于接收者任期，则拒绝追加日志
    if args.Term < rf.currentTerm {
        reply.Term = rf.currentTerm
        reply.Success = false
        return
    }
    // 如果接收者日志在PrevLogIndex处的日志条目的任期号和PrevLogTerm不匹配，则拒绝追加日志
    if rf.termAt(args.PrevLogIndex) != args.PrevLogTerm {
        reply.Term = rf.currentTerm
        reply.Success = false
        return
    }
    // 如果已经存在的日志条目和新的冲突（索引值相同但是任期号不同），则删除这一条和之后所有的条目，然后追加新的日志条目
    index := args.PrevLogIndex + 1
    for _, entry := range args.Entries {
        if rf.logs[index].Term != entry.Term {
            break
        }
        index++
    }
    if index <= rf.lastLogIndex() {
        rf.logs = rf.logs[:index]
    }
    rf.logs = append(rf.logs, args.Entries...)
    reply.Term = rf.currentTerm
    reply.Success = true

    // 如果领导者的提交索引大于接收者的提交索引，则更新接收者的提交索引为领导者的提交索引和接收者最后一条日志索引中较小的一个
    if args.LeaderCommit > rf.commitIndex {
        rf.commit(min(args.LeaderCommit, rf.lastLogIndex()))
    }
}

func (rf *Raft) commit(index int) {
    for i := rf.commitIndex + 1; i <= index; i++ {
        msg := ApplyMsg{
            Index:   i,
            Command: rf.logs[i].Command,
        }
        rf.applyCh <- msg
    }
    rf.commitIndex = index
}

func (rf *Raft) sendAppendEntriesTooAll() {
    for i := range rf.peers {
        if i != rf.me && rf.state == LEADER {
            go func(i int) {
                rf.mu.Lock()
                var reply AppendEntriesReply
                args := AppendEntriesArgs{
                    Term:         rf.currentTerm,
                    LeaderId:     rf.me,
                    PrevLogIndex: rf.nextIndex[i] - 1,
                    PrevLogTerm:  rf.termAt(rf.nextIndex[i] - 1),
                    Entries:      rf.logs[rf.nextIndex[i]:],
                    LeaderCommit: rf.commitIndex,
                }
                rf.mu.Unlock()
                ok := rf.peers[i].Call("Raft.AppendEntries", args, &reply)
                if ok {
                    rf.mu.Lock()
                    // 如果当前任期小于接收者任期，则更新当前任期，并转换为跟随者
                    if rf.currentTerm < reply.Term {
                        rf.currentTerm = reply.Term
                        rf.become(FOLLOWER)
                    }
                    if reply.Success {
                        // 如果追加日志成功，则更新matchIndex和nextIndex
                        newMatchIndex := args.PrevLogIndex + len(args.Entries)
                        rf.matchIndex[i] = newMatchIndex
                        rf.nextIndex[i] = newMatchIndex + 1
                        // 统计有多少服务器复制了该日志条目
                        cnt := 0
                        for _, index := range rf.matchIndex {
                            if newMatchIndex <= index {
                                cnt++
                            }
                        }
                        // 如果大多数服务器都已经复制了日志条目，则提交该日志条目
                        if cnt > len(rf.peers) / 2 {
                            rf.commit(newMatchIndex)
                        }
                    } else {
                        // 如果追加日志失败，则递减nextIndex，重新尝试追加日志
                        rf.nextIndex[i] = max(1, rf.nextIndex[i] - 1)
                    }
                    rf.mu.Unlock()
                }
            }(i)
        }
    }
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    defer rf.persist()
    if rf.state != LEADER {
        return -1, -1, false
    }
    rf.logs = append(rf.logs, LogEntry{
        Term:    rf.currentTerm,
        Command: command,
    })
    rf.nextIndex[rf.me] += 1
    rf.matchIndex[rf.me] += 1

    go rf.sendAppendEntriesTooAll()

    index := rf.lastLogIndex()
    term := rf.lastLogTerm()
	return index + 1, term, true
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
    defer rf.persist()
    rf.electionLoopDone <- true
    rf.heartbeatLoopDone <- true
}

func (rf* Raft) startElection() {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    args := RequestVoteArgs{
        Term: rf.currentTerm,
        CandidateId: rf.me,
        LastLogIndex: rf.lastLogIndex(),
        LastLogTerm: rf.lastLogTerm(),
    }
    for i := range rf.peers {
        if i != rf.me && rf.state == CANDIDATE {
            go func(i int) {
                var reply RequestVoteReply
                rf.sendRequestVote(i, args, &reply)
            }(i)
        }
    }
}

func (rf* Raft) heartbeatLoop() {
    for {
        select {
        case <- rf.heartbeatTicker.C:
            rf.sendAppendEntriesTooAll()
        case <- rf.heartbeatLoopDone:
            return
        }
    }
}

func (rf* Raft) electionLoop() {
    for {
        select {
        case <- rf.electionTicker.C:
            rf.mu.Lock()
            if rf.state == FOLLOWER || rf.state == CANDIDATE {
                rf.become(CANDIDATE)
            }
            rf.mu.Unlock()
        case <- rf.electionLoopDone:
            return
        }
    }
}

func randomTimeout() time.Duration {
    return time.Duration(rand.Int63n(int64(MAX_ELECTION_TIMEOUT - MIN_ELECTION_TIMEOUT))) + MIN_ELECTION_TIMEOUT
}

func (rf* Raft) resetElectionTicker() {
    if rf.electionTicker == nil {
        rf.electionTicker = time.NewTicker(randomTimeout())
    }
    rf.electionTicker.Reset(randomTimeout())
}

func (rf* Raft) resetHeartbeatTicker() {
    if rf.heartbeatTicker == nil {
        rf.heartbeatTicker = time.NewTicker(HEARTBEAT_INTERVAL)
    }
    rf.heartbeatTicker.Reset(HEARTBEAT_INTERVAL)
}

func (rf* Raft) stopElectionTicker() {
    if rf.electionTicker == nil {
        rf.electionTicker = time.NewTicker(randomTimeout())
    }
    rf.electionTicker.Stop()
}

func (rf* Raft) stopHeartBeatTicker() {
    if rf.heartbeatTicker == nil {
        rf.heartbeatTicker = time.NewTicker(HEARTBEAT_INTERVAL)
    }
    rf.heartbeatTicker.Stop()
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
    rf.currentTerm = 0
    rf.electionLoopDone = make(chan bool)
    rf.heartbeatLoopDone = make(chan bool)

    rf.become(FOLLOWER)
    go rf.heartbeatLoop()
    go rf.electionLoop()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
