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
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

type rf_index = uint32
type rf_term = uint32
type rf_ID = int16
type rf_status = int32
type rpc_ret = int32

const (
	election_min_time = time.Millisecond * 250
	heart_time        = time.Millisecond * 50
)

const (
	follower rf_status = iota
	candidate
	leader
)

const (
	complete rpc_ret = iota
	call_err
	log_err
	term_err
	get_vote
	no_vote
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  rf_term

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        //最外层锁          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	applych chan ApplyMsg

	status         rf_status     //mu锁
	leaderID       rf_ID         //mu锁
	isbeat         bool          //当leader
	election_timer *time.Timer   //mu锁
	election_time  time.Duration //mu锁
	next_index     rf_index      //index锁

	commit_cond *sync.Cond
	append_map  map[rf_index]int16 //mu锁 //的时候
	index_mu    sync.RWMutex       //二层锁
	index_cond  *sync.Cond

	leader_lastindex rf_index //mu锁//任期改变重置
	//持久性

	currentTerm rf_term   //mu锁
	votedFor    rf_ID     //mu锁
	logs        []Log     //index锁
	snapshot    *Snapshot //index锁

	//易失性

	commitIndex rf_index //mu锁
	lastApplied rf_index

	// 领导者易失性

	nextIndex []rf_index //重新初始化
	// matchIndex []rf_index
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}
type Log struct {
	Term    rf_term
	Index   rf_index
	Command interface{}
}
type Snapshot struct {
	LastIncludedIndex rf_index
	LastIncludedTerm  rf_term
	Data              []byte
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = int(rf.currentTerm)
	if rf.leaderID == int16(rf.me) {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
// 内部无锁，需加mu,index读锁
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	w1 := new(bytes.Buffer)
	e1 := labgob.NewEncoder(w1)
	e1.Encode(rf.currentTerm)
	e1.Encode(rf.votedFor)
	e1.Encode(rf.logs)
	e1.Encode(rf.snapshot.LastIncludedIndex)
	e1.Encode(rf.snapshot.LastIncludedTerm)
	raftstate := w1.Bytes()
	if rf.snapshot.LastIncludedIndex != 0 {
		rf.persister.Save(raftstate, rf.snapshot.Data)
	} else {
		rf.persister.Save(raftstate, nil)
	}
}

// restore previously persisted state.
// 内部有锁
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm rf_term
	var votedFor rf_ID
	var logs []Log
	var LastIncludedIndex rf_index
	var LastIncludedTerm rf_term
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&LastIncludedIndex) != nil ||
		d.Decode(&LastIncludedTerm) != nil {
		fmt.Println(rf.me, "无可读取持久化")
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.index_mu.Lock()
		defer rf.index_mu.Unlock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.snapshot.LastIncludedIndex = LastIncludedIndex
		rf.snapshot.LastIncludedTerm = LastIncludedTerm
		if len(rf.logs) > 0 {
			rf.next_index = rf.logs[len(logs)-1].Index + 1
			for i := range rf.nextIndex {
				if i != rf.me {
					rf.nextIndex[i] = rf.next_index
				}
			}
		} else if rf.snapshot.LastIncludedIndex > 0 {
			rf.next_index = rf.snapshot.LastIncludedIndex + 1
			for i := range rf.nextIndex {
				if i != rf.me {
					rf.nextIndex[i] = rf.next_index
				}
			}
		}
	}
}
func (rf *Raft) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.snapshot.Data = data
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	if index <= int(rf.snapshot.LastIncludedIndex) {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.index_mu.Lock()
	defer rf.index_mu.Unlock()
	if len(rf.logs) > 0 {
		i := rf_index(index) - rf.logs[0].Index
		// fmt.Println(rf.me, "生成快照", i, rf_index(index), rf.logs[0].Index, "commit才", rf.commitIndex)
		if i >= uint32(len(rf.logs)) {
			return
		}
		rf.snapshot.Data = snapshot
		rf.snapshot.LastIncludedIndex = rf.logs[i].Index
		rf.snapshot.LastIncludedTerm = rf.logs[i].Term
		if rf_index(len(rf.logs)-1) > i {
			tmp := make([]Log, rf_index(len(rf.logs)-1)-i)
			copy(tmp, rf.logs[i+1:])
			rf.logs = tmp
		} else if rf_index(len(rf.logs)-1) == i {
			rf.logs = make([]Log, 0)
		}
		// fmt.Println(rf.me, rf.snapshot.LastIncludedIndex, "快照后", rf.logs)
		rf.persist()
	}
}

type InstallSnapshotArgs struct {
	Term              rf_term
	LeaderId          rf_ID
	LastIncludedIndex rf_index
	LastIncludedTerm  rf_term
	Data              []byte
}
type InstallSnapshotReply struct {
	Term rf_term
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if rf.killed() {
		return
	}
	ispersist := false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		if ispersist {
			rf.persist()
		}
	}()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	} else {
		if rf.status == leader {
			go rf.ticker()
		}
		if args.Term > rf.currentTerm {
			rf.votedFor = -1
			rf.currentTerm = args.Term
			rf.leader_lastindex = 0
			ispersist = true
		}
		rf.status = follower
		rf.leaderID = args.LeaderId
		reply.Term = rf.currentTerm
		rf.index_mu.Lock()
		defer rf.index_mu.Unlock()
		if args.LastIncludedIndex <= rf.commitIndex {
			// fmt.Println(rf.me, "过期快照", rf.commitIndex, args.LastIncludedIndex, args.LastIncludedTerm, rf.logs)
			return
		} else {
			rf.election_timer.Reset(rf.election_time)
			rf.next_index = args.LastIncludedIndex + 1
			rf.leader_lastindex = args.LastIncludedIndex
			// for i := range rf.logs {
			// 	if rf.logs[i].Term > args.LastIncludedTerm ||
			// 		(rf.logs[i].Term == args.LastIncludedTerm &&
			// 			rf.logs[i].Index > args.LastIncludedIndex) {
			// 		tmp := make([]Log, len(rf.logs)-i)
			// 		copy(tmp, rf.logs[i:])
			// 		rf.logs = tmp
			// 	} else if i == (len(rf.logs) - 1) {
			// 		rf.logs = make([]Log, 0)
			// 	}
			// }
			rf.logs = make([]Log, 0)
			// fmt.Println(rf.me, "安装快照", args.LastIncludedIndex)
			rf.snapshot.Data = args.Data
			rf.snapshot.LastIncludedIndex = args.LastIncludedIndex
			rf.snapshot.LastIncludedTerm = args.LastIncludedTerm
			rf.commitIndex = args.LastIncludedIndex
			rf.commit_cond.Broadcast()
			ispersist = true
			// fmt.Println(rf.me, rf.snapshot.LastIncludedIndex, "应用快照后", rf.logs)
		}
	}
}
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) rpc_ret {
	if rf.peers[server].Call("Raft.InstallSnapshot", args, reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if args.Term < rf.currentTerm {
			return term_err
		} else if reply.Term > args.Term {
			if rf.status == leader {
				go rf.ticker()
			}
			rf.status = follower
			rf.currentTerm = reply.Term
			rf.leader_lastindex = 0
			rf.leaderID = -1
			rf.votedFor = -1
			rf.index_mu.RLock()
			rf.persist()
			rf.index_mu.RUnlock()
			return term_err
		} else {
			return complete
		}
	} else {
		return call_err
	}
}

type AppendEntriesArgs struct {
	Term         rf_term
	LeaderId     rf_ID
	PrevLogIndex rf_index
	PrevLogTerm  rf_term
	Entries      []Log
	LeaderCommit rf_index
}
type AppendEntriesReply struct {
	Term    rf_term
	Success bool
	XTerm   rf_term
	XIndex  rf_index
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		return
	}
	ispersist := false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		if ispersist {
			rf.index_mu.RLock()
			rf.persist()
			rf.index_mu.RUnlock()
		}
	}()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
	} else {
		if rf.status == leader {
			go rf.ticker()
		}
		if args.Term > rf.currentTerm {
			rf.votedFor = -1
			rf.currentTerm = args.Term
			rf.leader_lastindex = 0
			ispersist = true
		}
		rf.status = follower
		rf.leaderID = args.LeaderId
		reply.Success = true
		reply.Term = rf.currentTerm
		if rf.leader_lastindex >= args.LeaderCommit {
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = args.LeaderCommit //乱序rpc小概率出事（已改）
				rf.commit_cond.Broadcast()
			}
		}
		if len(args.Entries) > 0 {
			rf.index_mu.Lock()
			if args.PrevLogIndex >= rf.snapshot.LastIncludedIndex {
				if tmp := rf.find_log(args.PrevLogIndex); args.PrevLogIndex == 0 ||
					(tmp != nil && args.PrevLogTerm == tmp.Term) ||
					(tmp == nil && rf.snapshot.LastIncludedIndex == args.PrevLogIndex && rf.snapshot.LastIncludedTerm == args.PrevLogTerm) {
					// fmt.Println(rf.me, "收到append")
					for i, val := range args.Entries {
						if tmp2 := rf.find_log(args.PrevLogIndex + uint32(i) + 1); tmp2 == nil {
							rf.election_timer.Reset(rf.election_time)
							rf.logs = append(rf.logs, val)
							rf.next_index = val.Index + 1
							rf.leader_lastindex = val.Index
						} else if tmp2.Term != val.Term {
							rf.election_timer.Reset(rf.election_time)
							// fmt.Println(rf.me, "截断至", args.PrevLogIndex-rf.logs[0].Index+uint32(i)+1)
							rf.logs = rf.logs[:args.PrevLogIndex-rf.logs[0].Index+uint32(i)+1]
							rf.logs = append(rf.logs, val)
							rf.next_index = val.Index + 1
							rf.leader_lastindex = val.Index
						} //过期日志不处理
					}
					if rf.leader_lastindex >= args.LeaderCommit {
						if args.LeaderCommit > rf.commitIndex {
							rf.commitIndex = args.LeaderCommit
							rf.commit_cond.Broadcast()
						}
					}
					ispersist = true
				} else if tmp != nil {
					rf.election_timer.Reset(rf.election_time)
					// fmt.Println(rf.me, "中", tmp.Term, tmp.Index, "不等于", args.PrevLogTerm, args.PrevLogIndex)
					if tmp.Term < args.PrevLogTerm {
						reply.Success = false
						reply.XTerm = tmp.Term
						reply.XIndex = tmp.Index
					} else {
						reply.Success = false
						reply.XIndex = rf.snapshot.LastIncludedIndex + 1
						for i := tmp.Index - 1; i > 0; i-- {
							if tmp2 := rf.find_log(i); tmp2 != nil && tmp2.Term < args.PrevLogTerm {
								// fmt.Println("跟随者", rf.me, "tmp", tmp2.Term, "args", args.PrevLogTerm)
								reply.XIndex = tmp2.Index
								reply.XTerm = tmp2.Term
								break
							} else if tmp2 == nil {
								tmp2 = rf.find_log(i + 1)
								reply.XIndex = tmp2.Index
								reply.XTerm = tmp2.Term
								break
							}
						}
					}
					if len(rf.logs) > 0 {
						// fmt.Println(rf.me, "截断至", reply.XIndex-rf.logs[0].Index)
						// if reply.XIndex-rf.logs[0].Index > uint32(cap(rf.logs)) {
						// 	fmt.Println(reply, rf.logs[0].Index)
						// }
						rf.logs = rf.logs[:reply.XIndex-rf.logs[0].Index]
						if len(rf.logs) > 0 {
							rf.next_index = rf.logs[len(rf.logs)-1].Index + 1
						} else {
							rf.next_index = rf.snapshot.LastIncludedIndex + 1
						}
					}
				} else {
					rf.election_timer.Reset(rf.election_time)
					reply.Success = false
					reply.XTerm = 0
					if len(rf.logs) == 0 && rf.snapshot.LastIncludedIndex == 0 {
						reply.XIndex = 1
					} else if len(rf.logs) == 0 && rf.snapshot.LastIncludedIndex > 0 {
						// fmt.Println(rf.me, "err", rf.snapshot.LastIncludedIndex)
						reply.XIndex = rf.snapshot.LastIncludedIndex
					} else {
						reply.XIndex = rf.logs[len(rf.logs)-1].Index
					}
				}
			}
			rf.index_mu.Unlock()
		} else {
			if args.LeaderCommit >= rf.commitIndex {
				rf.election_timer.Reset(rf.election_time)
			}
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) rpc_ret {
	if rf.peers[server].Call("Raft.AppendEntries", args, reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if args.Term < rf.currentTerm {
			return term_err
		} else if !reply.Success {
			if reply.Term > args.Term {
				if rf.status == leader {
					go rf.ticker()
				}
				rf.status = follower
				rf.currentTerm = reply.Term
				rf.leader_lastindex = 0
				rf.leaderID = -1
				rf.votedFor = -1
				rf.index_mu.RLock()
				rf.persist()
				rf.index_mu.RUnlock()
				return term_err
			} else {
				return log_err
			}
		} else {
			return complete
		}
	} else {
		return call_err
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         rf_term
	CandidateId  rf_ID
	LastLogIndex rf_index
	LastLogTerm  rf_term
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        rf_term
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	if rf.killed() {
		return
	}
	ispersist := false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.index_mu.RLock()
	defer rf.index_mu.RUnlock()
	defer func() {
		if ispersist {
			rf.persist()
		}
	}()

	// fmt.Println(rf.me, "在任期", args.Term, "收到", args.CandidateId, "请求")
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else {
		if args.Term > rf.currentTerm {
			if rf.status == leader {
				go rf.ticker()
			}
			rf.currentTerm = args.Term
			rf.leader_lastindex = 0
			rf.status = follower
			rf.leaderID = -1
			rf.votedFor = -1
			ispersist = true
		}
		if rf.votedFor == args.CandidateId {
			// fmt.Println(rf.me, "在任期", args.Term, "投给", args.CandidateId)
			reply.VoteGranted = true
		} else if rf.votedFor == -1 {
			// if args.LastLogIndex != 0 && len(rf.logs) == 0 {
			// 	fmt.Println(args.CandidateId, "lastlog=", args.LastLogIndex, "to", rf.me)
			// 	fmt.Println(rf.logs, rf.commitIndex, rf.next_index)
			// }
			if len(rf.logs) == 0 ||
				args.LastLogTerm > rf.logs[len(rf.logs)-1].Term ||
				(args.LastLogTerm == rf.logs[len(rf.logs)-1].Term &&
					args.LastLogIndex >= rf.logs[len(rf.logs)-1].Index) {
				// fmt.Println(rf.me, "在任期", args.Term, "投给", args.CandidateId)
				rf.reset_ticker()
				reply.VoteGranted = true
				rf.votedFor = args.CandidateId
				ispersist = true
			}
		}
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) rpc_ret {
	if rf.peers[server].Call("Raft.RequestVote", args, reply) {
		ispersist := false
		rf.mu.Lock()
		defer rf.mu.Unlock()
		defer func() {
			if ispersist {
				rf.index_mu.RLock()
				rf.persist()
				rf.index_mu.RUnlock()
			}
		}()
		if !reply.VoteGranted {
			if reply.Term > rf.currentTerm {
				rf.status = follower
				rf.currentTerm = reply.Term
				rf.leader_lastindex = 0
				rf.leaderID = -1
				rf.votedFor = -1
				ispersist = true
			}
			return no_vote
		} else {
			// fmt.Println(rf.me, "拿到", args.Term, "任期", server, "的票")
			if args.Term < rf.currentTerm {
				return no_vote
			}
			return get_vote
		}
	} else {
		return call_err
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
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status != leader {
		isLeader = false
		return index, term, isLeader
	} else {
		index = int(rf.next_index)
		term = int(rf.currentTerm)
		// fmt.Println("Start", rf.next_index)
		rf.index_mu.Lock()
		rf.logs = append(rf.logs, Log{Term: rf.currentTerm, Index: rf.next_index, Command: command})
		rf.persist()
		rf.next_index++
		rf.index_cond.Broadcast()
		rf.index_mu.Unlock()
		return index, term, isLeader
	}
}

// 函数未加锁,需外部加上index读锁
func (rf *Raft) find_log(index rf_index) *Log {
	if len(rf.logs) == 0 {
		return nil
	}
	if i := rf.logs[0].Index; index >= i && (index-i) <= uint32(len(rf.logs)-1) {
		index = index - i
		log := new(Log)
		*log = rf.logs[index]
		return log
	}
	return nil
}
func (rf *Raft) deal_append(server int) {
	num := 1
	var term rf_term
	for !rf.killed() {
		rf.index_mu.RLock()
		for rf.status != leader || rf.next_index <= rf.nextIndex[server] {
			rf.index_cond.Wait()
			if rf.killed() {
				return
			}
		}
		if term < rf.currentTerm {
			num = 1
		}

		term = rf.currentTerm
		// t1 := time.Now()
		if len(rf.logs) > 0 && rf.logs[0].Index <= rf.nextIndex[server] {
			args := AppendEntriesArgs{Term: term, LeaderId: int16(rf.me),
				PrevLogIndex: rf.nextIndex[server] - 1, LeaderCommit: rf.commitIndex}
			for i := 0; i < num; i++ {
				// if rf.find_log(rf.nextIndex[server]+uint32(i)) == nil {
				// 	fmt.Println(rf.nextIndex[server], uint32(i), len(rf.logs), rf.snapshot.LastIncludedIndex, rf.logs[0].Index)
				// }
				args.Entries = append(args.Entries, *rf.find_log(rf.nextIndex[server] + uint32(i)))
			}
			if log := rf.find_log(rf.nextIndex[server] - 1); log != nil {
				args.PrevLogTerm = log.Term
			} else {
				args.PrevLogTerm = rf.snapshot.LastIncludedTerm
			}
			rf.index_mu.RUnlock()
			var reply AppendEntriesReply
			switch rf.sendAppendEntries(server, &args, &reply) {
			case log_err:
				// fmt.Println("befor index", rf.nextIndex[server])
				if reply.XTerm == 0 {
					rf.nextIndex[server] = reply.XIndex
				} else {
					if reply.XIndex < (rf.nextIndex[server] + uint32(num)) {
						rf.nextIndex[server] = reply.XIndex
					}
					rf.index_mu.RLock()
					// fmt.Println(server, "next", rf.nextIndex[server])
					for i := rf.nextIndex[server]; i >= rf.snapshot.LastIncludedIndex; i-- {
						if tmp := rf.find_log(i); tmp != nil && tmp.Term <= reply.XTerm {
							// fmt.Println(server, "tmp", tmp.Term, "reply", reply.XTerm)
							rf.nextIndex[server] = i
							break
						} else if tmp == nil {
							rf.nextIndex[server] = i
							break
						} else if i == 1 {
							rf.nextIndex[server] = i
							break
						}
					}
					rf.index_mu.RUnlock()
				}
				// fmt.Println(server, "after index", rf.nextIndex[server])
				num = 1
			case complete:
				// fmt.Println(server, rf.nextIndex[server]+uint32(num)-1, "append成功")
				rf.isbeat = true
				rf.index_mu.RLock()
				if log := rf.find_log(rf.nextIndex[server]); log != nil &&
					log.Term == rf.currentTerm && //没有3B也能过
					rf.nextIndex[server] > rf.commitIndex {
					rf.index_mu.RUnlock()
					rf.mu.Lock()
					for i := rf.nextIndex[server] + uint32(num) - 1; i >= rf.nextIndex[server]; i-- {
						_, ok := rf.append_map[i]
						if ok {
							rf.append_map[i]++
						} else {
							rf.append_map[i] = 2 //加上自己
						}
						if rf.append_map[i] > int16(len(rf.peers)>>1) {
							// fmt.Println(i, "日志提交")
							count := i - rf.commitIndex
							for c := 0; c < int(count); c++ {
								rf.commitIndex++
								delete(rf.append_map, rf.commitIndex)
							}
							if count > 0 {
								rf.commit_cond.Broadcast()
							}
							break
						}
					}
					rf.mu.Unlock()
					rf.index_mu.RLock()
				}
				rf.nextIndex[server] += uint32(num)
				if rf.nextIndex[server] < rf.next_index {
					num = int(rf.next_index - rf.nextIndex[server])
				} else {
					num = 1
				}
				rf.index_mu.RUnlock()
			case call_err:
				// fmt.Println(server, "rpc失败")
			case term_err:
				num = 1
			}
		} else if rf.snapshot.LastIncludedIndex > 0 && rf.nextIndex[server] <= rf.snapshot.LastIncludedIndex {
			if len(rf.logs) > 0 {
				rf.nextIndex[server] = rf.logs[0].Index
			} else {
				rf.nextIndex[server] = rf.snapshot.LastIncludedIndex + 1
			}
			args := InstallSnapshotArgs{Term: term, LeaderId: int16(rf.me),
				LastIncludedIndex: rf.snapshot.LastIncludedIndex,
				LastIncludedTerm:  rf.snapshot.LastIncludedTerm,
				Data:              rf.snapshot.Data}
			// fmt.Println("向", server, "发送快照", rf.snapshot.LastIncludedIndex)
			rf.index_mu.RUnlock()
			var reply InstallSnapshotReply
			rf.sendInstallSnapshot(server, &args, &reply)
			num = 1
		}
		// fmt.Println(time.Since(t1))
	}
}
func (rf *Raft) deal_apply() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied {
			rf.commit_cond.Wait()
			if rf.killed() {
				return
			}
		}
		rf.mu.Unlock()
		var apply ApplyMsg
		rf.index_mu.RLock()
		if rf.commitIndex > rf.lastApplied && rf.lastApplied < rf.snapshot.LastIncludedIndex {
			// fmt.Println(rf.me, "apply快照", rf.snapshot.LastIncludedIndex)
			rf.lastApplied = rf.snapshot.LastIncludedIndex
			apply.CommandValid = false
			apply.SnapshotValid = true
			apply.SnapshotIndex = int(rf.snapshot.LastIncludedIndex)
			apply.SnapshotTerm = int(rf.snapshot.LastIncludedTerm)
			apply.Snapshot = rf.snapshot.Data
			rf.index_mu.RUnlock()
			rf.applych <- apply
		} else {
			msgs := make([]ApplyMsg, 0)
			for rf.commitIndex > rf.lastApplied {
				tmp := rf.find_log(rf.lastApplied + 1)
				if tmp != nil {
					rf.lastApplied++
					apply.Command = tmp.Command
					apply.CommandIndex = int(tmp.Index)
					apply.CommandValid = true
					apply.CommandTerm = tmp.Term
					msgs = append(msgs, apply)
					// fmt.Println(rf.me, "apply", apply.CommandIndex, rf.lastApplied)
					// if apply.CommandIndex != int(rf.lastApplied) {
					// 	fmt.Println(rf.logs)
					// }
				} else {
					break
				}
			}
			rf.index_mu.RUnlock()
			for _, msg := range msgs {
				rf.applych <- msg
			}
		}
	}
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
	// Your code here, if desired.
	rf.commit_cond.Broadcast()
	rf.index_cond.Broadcast()
	fmt.Println(rf.me, "log_len", len(rf.logs))
	fmt.Println("kill", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) heart_beat() {
	var args AppendEntriesArgs
	args.LeaderId = int16(rf.me)
	args.Term = rf.currentTerm
	args.LeaderCommit = rf.commitIndex
	for i := range rf.peers {
		tmp := i
		if i != rf.me {
			go func(rf *Raft) {
				var reply AppendEntriesReply
				rf.sendAppendEntries(tmp, &args, &reply)
			}(rf)
		}
	}
	for !rf.killed() {
		time.Sleep(heart_time)
		rf.mu.RLock()
		if rf.status != leader || rf.killed() {
			rf.mu.RUnlock()
			return
		}
		if rf.isbeat {
			rf.isbeat = false
			rf.mu.RUnlock()
			continue
		}
		args.LeaderCommit = rf.commitIndex
		rf.mu.RUnlock()
		for i := range rf.peers {
			tmp := i
			if i != rf.me {
				go func(rf *Raft) {
					var reply AppendEntriesReply
					rf.sendAppendEntries(tmp, &args, &reply)
				}(rf)
			}
		}
	}
}

func (rf *Raft) reset_ticker() bool {
	ms := rand.Int63() % int64(30*len(rf.peers))
	rf.election_time = election_min_time + time.Duration(ms)*time.Millisecond
	return rf.election_timer.Reset(rf.election_time)
}
func (rf *Raft) ticker() {
	kill_ticker := make(chan int, 1)
	for !rf.killed() {
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		rf.election_timer = time.NewTimer(election_min_time)
		rf.reset_ticker()
		rf.mu.Unlock()
		select {
		case <-rf.election_timer.C:
		case <-kill_ticker:
			return
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		go func() {
			rf.mu.Lock()
			if rf.status == leader || rf.killed() {
				rf.mu.Unlock()
				kill_ticker <- 0
				return
			}
			rf.status = candidate
			rf.leaderID = -1
			rf.currentTerm++
			fmt.Println(rf.me, "超时", "cur", rf.currentTerm)
			rf.leader_lastindex = 0
			rf.votedFor = int16(rf.me)
			rf.persist()
			term := rf.currentTerm
			var args RequestVoteArgs
			args.Term = term
			args.CandidateId = int16(rf.me)
			rf.index_mu.RLock()
			if len(rf.logs) > 0 {
				args.LastLogIndex = rf.logs[len(rf.logs)-1].Index
				args.LastLogTerm = rf.logs[len(rf.logs)-1].Term
			} else {
				args.LastLogIndex = rf.snapshot.LastIncludedIndex
				args.LastLogTerm = rf.snapshot.LastIncludedTerm
			}
			rf.index_mu.RUnlock()
			rf.mu.Unlock()
			wait := make(chan int, len(rf.peers)-1)
			var ret int32 = 0
			var vote_num uint32 = 1
			for i := range rf.peers {
				if i != rf.me {
					tmp := i
					go func() {
						defer func() {
							recover()
						}()
						defer func() {
							wait <- 0
						}()
						defer atomic.AddInt32(&ret, 1)
						var reply RequestVoteReply
						switch rf.sendRequestVote(tmp, &args, &reply) { //rpc问题 一直堵塞/延迟很久返回
						case get_vote:
							atomic.AddUint32(&vote_num, 1)
						}
					}()
				}
			}
			for {
				<-wait
				if vote_num > uint32(len(rf.peers)>>1) || ret == int32(len(rf.peers)-1) {
					close(wait)
					break
				}
			}
			// fmt.Println(vote_num, "比", len(rf.peers)>>1)
			if vote_num > uint32(len(rf.peers)>>1) && term == rf.currentTerm {
				fmt.Println(rf.me, "get", rf.currentTerm)
				rf.mu.Lock()
				kill_ticker <- 0
				rf.leaderID = int16(rf.me)
				rf.status = leader
				rf.mu.Unlock()
				go rf.heart_beat()
				rf.become_leader()
				return
			}
		}()
	}
}

// 内有index和mu锁
func (rf *Raft) become_leader() {
	rf.isbeat = false
	rf.mu.Lock()
	rf.append_map = make(map[uint32]int16)
	rf.mu.Unlock()
	rf.index_mu.Lock()
	defer rf.index_mu.Unlock()
	rf.nextIndex = make([]uint32, len(rf.peers))
	for i := range rf.nextIndex {
		if i != rf.me {
			rf.nextIndex[i] = rf.next_index
		}
	}
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
	// fmt.Println("活")
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.snapshot = &Snapshot{LastIncludedIndex: 0, LastIncludedTerm: 0}
	rf.applych = applyCh

	rf.status = follower
	rf.leaderID = -1
	rf.isbeat = false
	rf.leader_lastindex = 0

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.next_index = 1

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.commit_cond = sync.NewCond(&rf.mu)
	rf.index_cond = sync.NewCond(rf.index_mu.RLocker())

	rf.nextIndex = make([]uint32, len(rf.peers))
	rf.append_map = make(map[uint32]int16)
	for i := range rf.nextIndex {
		if i != rf.me {
			rf.nextIndex[i] = rf.next_index
			go rf.deal_append(i)
		}
	}
	go rf.deal_apply()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
