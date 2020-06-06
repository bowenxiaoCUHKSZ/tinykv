// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"fmt"
	"math/rand"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	// "github.com/pingcap/kvproto/pkg/eraftpb"
	// "github.com/prometheus/common/log"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// number of ticks since it reached last electionTimeout
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	peers []uint64
	
	oldTerm uint64

	countOfAppend int
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	raft := &Raft{}
	raft.id = c.ID
	raft.electionElapsed = 0
	raft.electionTimeout = c.ElectionTick
	raft.heartbeatElapsed = 0
	raft.heartbeatTimeout = c.HeartbeatTick
	raft.peers = c.peers
	raft.State = StateFollower
	raft.votes = make(map[uint64]bool)

	// old Term
	raft.oldTerm = 0

	// Raft Log
	raft.RaftLog = newLog(c.Storage)

	// if raft == nil{
	// 	fmt.Printf("Why raft is nil\n")
	// }
	oldHardState, _, _ := c.Storage.(*MemoryStorage).InitialState()
	raft.Vote = oldHardState.Vote
	raft.Term = oldHardState.Term
	raft.countOfAppend = 0
	fmt.Printf("")
	return raft
}

func (r *Raft) sendAppendToPeers() {
	r.countOfAppend = 0

	for _, v := range r.peers{
		if r.id == v{
			continue
		}
		r.sendAppend(v)
	}
}
// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	msgs := getMessage(pb.MessageType_MsgAppend, r.id, to)

	// newEntry := &pb.Entry{}
	// newEntry.Term = r.Term
	// newEntry.Index = r.RaftLog.LastIndex()+1
	// r.RaftLog.entries = append(r.RaftLog.entries, *newEntry)	
	// copy = append(copy, *newEntry)
	for _, entry := range r.RaftLog.entries{
		ent := pb.Entry{}
		ent.Index = entry.Index
		ent.Term = entry.Term
		ent.Data = entry.Data
		// fmt.Printf("to:%d index:%d term:%d\n", to, (&entry).Index, (&entry).Term)

		msgs.Entries = append(msgs.Entries, &ent)
	}
	msgs.Term = r.Term
	// fmt.Printf("from: %d, msgs.Commit : %d\n", r.id, r.RaftLog.committed)
	msgs.Commit = r.RaftLog.LastIndex()

	r.msgs = append(r.msgs, *msgs)
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	msg := getMessage(pb.MessageType_MsgHeartbeat, r.id, to)
	msg.Term = r.Term
	r.msgs = append(r.msgs, *msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	r.electionElapsed++
	if r.electionElapsed == r.electionTimeout{
		// r.sendHeartbeat(0)
		msg := pb.Message{}
		msg.MsgType = pb.MessageType_MsgHup
		r.Step(msg)
		// r.resetElectionTimeOut()
		// r.electionElapsed = 0
	}

	r.heartbeatElapsed++
	if r.heartbeatElapsed == r.heartbeatTimeout{
		r.sendHeartbeats()
		// r.heartbeatElapsed = 0
	}
}

func (r *Raft) sendHeartbeats() {
	for _, v := range r.peers{
		if v == r.id{
			continue
		}
		r.sendHeartbeat(v)
	}
}
// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Lead = lead

	for k, _ := range r.votes{
		r.votes[k] = false
	}

	r.resetElectionTimeOut()
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	if r.oldTerm == r.Term{
		r.Term++
	}
	// r.RaftLog.committed++
	r.State = StateCandidate
	r.resetElectionTimeOut()
}


func (r *Raft) resetElectionTimeOut()  {
	r.electionTimeout = 10 + rand.Intn(10) 
	r.electionElapsed = 0
}
// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	// r.Term++
	if r.oldTerm == r.Term{
		r.Term++
	}
	r.State = StateLeader
	r.oldTerm++

	// init for progress
	r.Prs = make(map[uint64]*Progress)
	for _, v:= range r.peers{
		r.Prs[v] = &Progress{Match:1, Next:2}
	}

	// change Log
	// fmt.Printf("in become Leader: %d\n", r.RaftLog.LastIndex())
	newEntry := &pb.Entry{}
	newEntry.Term = r.Term
	newEntry.Index = r.RaftLog.LastIndex()+1
	r.RaftLog.entries = append(r.RaftLog.entries, *newEntry)	
	// fmt.Printf("in become Leader: %d\n", r.RaftLog.LastIndex())
	r.RaftLog.committed += r.RaftLog.LastIndex() - r.RaftLog.committed

	r.sendAppendToPeers()
	
	
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		switch m.MsgType{
		case pb.MessageType_MsgHup:
			// electition begin
			// r.RaftLog.committed++
			r.StartElection(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(&m)

		case pb.MessageType_MsgRequestVoteResponse:

		case pb.MessageType_MsgHeartbeat:
			msg := getMessage(pb.MessageType_MsgHeartbeatResponse, r.id, m.From)
			if m.Commit > r.RaftLog.committed{
				r.RaftLog.committed = m.Commit
			}

			r.msgs = append(r.msgs, *msg)

		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)

		}
	case StateCandidate:
		switch m.MsgType{
		case pb.MessageType_MsgRequestVote:
			msg := pb.Message{}
			msg.MsgType = pb.MessageType_MsgRequestVoteResponse
			msg.From = r.id
			msg.To = m.From
			msg.Reject = true
			msg.Term = r.Term
			if m.Term > r.Term{
				r.becomeFollower(m.Term, m.From)
				msg.Reject = false
				r.Vote = m.From
			}else{
				msg.Reject = true
			}

			r.msgs = append(r.msgs, msg)

		case pb.MessageType_MsgRequestVoteResponse:

			// handle election fail
			lastLogIndex:= r.RaftLog.LastIndex()
			lastLogTerm,_ := r.RaftLog.Term(lastLogIndex)
			if m.Term > r.Term{
				r.becomeFollower(m.Term, m.From)
				return nil
			}else if m.Term == r.Term{
				if m.Index == lastLogIndex{
					if lastLogTerm < m.LogTerm{
						r.becomeFollower(m.Term, m.From)
						return nil
					}
				}
			}


			r.votes[m.From]= !m.Reject
	
			r.checkLeader()
			
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)

		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)

		case pb.MessageType_MsgHup:
			// electition begin
			r.StartElection(m)
		}
		
	case StateLeader:
		switch m.MsgType{
		case pb.MessageType_MsgRequestVote:
			msg := pb.Message{}
			msg.MsgType = pb.MessageType_MsgRequestVoteResponse
			msg.From = r.id
			msg.To = m.From
			

			if m.Term > r.Term{

				r.becomeFollower(m.Term, m.From)

				msg.Reject = false

				r.Vote = m.From
			}else{
				msg.Reject = true
			}

			r.msgs = append(r.msgs, msg)

		case pb.MessageType_MsgAppendResponse:
			if m.Term > r.Term{
				r.becomeFollower(m.Term, m.From)
			}

			// if the majority
			if !m.Reject{
				r.countOfAppend++
			} 

			if r.countOfAppend > len(r.peers) / 2 - 1{
				r.RaftLog.committed += r.RaftLog.LastIndex()  - r.RaftLog.committed
			}


			

		case pb.MessageType_MsgBeat:
			var msgs []pb.Message
			for _, v := range r.peers{
				if v == r.id{
					continue
				}
				vote := pb.Message{}
				vote.MsgType = pb.MessageType_MsgHeartbeat
				vote.Term = r.Term
				vote.From = r.id
				vote.To = uint64(v)
				
				// append message into queue
				// wait for deliver
				msgs = append(msgs, vote)
			}

			r.msgs = append(r.msgs, msgs...)

		case pb.MessageType_MsgHup:
			// do nothing

		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)

		case pb.MessageType_MsgPropose:
			r.Prs[r.id].Match++
			r.Prs[r.id].Next++

			// r.RaftLog.committed = r.RaftLog.LastIndex()+2

			// incorperate new message
			var cnt uint64
			for _, v := range m.Entries{
				newEntry := pb.Entry{}
				newEntry.Data = v.Data
				newEntry.Index = r.RaftLog.LastIndex()+1
				newEntry.Term = r.Term
				r.RaftLog.entries = append(r.RaftLog.entries, newEntry)

				if v.Data != nil{
					cnt++
				}
			}


			// RaftLog.committed should not be updated now
			// but sa
			// r.RaftLog.committed += cnt
			if len(r.peers) == 1{
				r.RaftLog.committed = r.RaftLog.LastIndex()
			}
			
			r.sendAppendToPeers()

		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	msg := getMessage(pb.MessageType_MsgAppendResponse, r.id, m.From)
	msg.Term = r.Term

	// fmt.Printf("%d %d\n", r.Term, m.Term)
	if r.Term > m.Term{
		// r.msgs = append(r.msgs, *msg)
	}else{
		
		r.becomeFollower(m.Term, m.From)
		// overwrite local entries
		var newEntries []pb.Entry
		for _, v := range m.Entries{
			// if v.Data != nil{
			// 	fmt.Printf("Yes!!\n")
			// }
			// fmt.Printf("recieve:%d, index:%d, term:%d\n", r.id, v.Index, v.Term)
			newEntries = append(newEntries, *v)
		}
		r.RaftLog.entries = newEntries
		r.RaftLog.committed = m.Commit
	}
	// r.RaftLog.committed = m.Commit
	r.msgs = append(r.msgs, *msg)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	
	// become a Follower
	r.becomeFollower(m.Term, m.From)

	msg := getMessage(pb.MessageType_MsgHeartbeatResponse, r.id, m.From)
	if m.Commit > r.RaftLog.committed{
		r.RaftLog.committed = m.Commit
	}

	r.msgs = append(r.msgs, *msg)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}


func (r *Raft) checkLeader(){
	cnt := 0
	for _, v := range r.votes{
		if v{
			cnt++
		}
	}

	if cnt >= len(r.peers)/2+1{
		r.becomeLeader()
	}	
}

func (r *Raft) StartElection(m pb.Message) {
	r.Term++
	// propose := r.Term
	r.msgs = make([]pb.Message, 0)

	//construct vote messages
	var msgs []pb.Message
	for _, v := range r.peers{
		if v == r.id{
			continue
		}
		vote := pb.Message{}
		vote.MsgType = pb.MessageType_MsgRequestVote
		vote.Term = r.Term
		vote.From = r.id
		vote.To = uint64(v)
		vote.LogTerm,_ = r.RaftLog.Term(r.RaftLog.LastIndex())
		vote.Index = r.RaftLog.LastIndex()
		// append message into queue
		// wait for deliver
		msgs = append(msgs, vote)
	}

	r.becomeCandidate()
	// vote for myself
	r.votes[r.id] = true

	r.checkLeader()	

	r.msgs = append(r.msgs, msgs...)
}
func getMessage(MsgType pb.MessageType, From, To uint64) *pb.Message{
	m := &pb.Message{}
	m.MsgType = MsgType
	m.From = From
	m.To = To
	return m
}

func (r *Raft) handleRequestVote(m *pb.Message){
	// Your code here (2A, 2B).

	msgs := getMessage(pb.MessageType_MsgRequestVoteResponse, r.id, m.From)
	lastLogTerm, _:= r.RaftLog.Term(r.RaftLog.LastIndex())
	lastLogIndex := r.RaftLog.LastIndex()
	msgs.Term = r.Term
	msgs.Reject = true

	// 
	msgs.LogTerm = lastLogTerm
	msgs.Index = lastLogIndex

	if m.Term < r.Term {
		r.msgs = append(r.msgs, *msgs)
		return
	} else if m.Term == r.Term {
		if r.State == StateLeader {
			r.msgs = append(r.msgs, *msgs)
			return
		}
		if r.Vote == m.From {
			msgs.Reject = false
			r.msgs = append(r.msgs, *msgs)
			return
		}
		if r.Vote != 0 && r.Vote != m.From {
			// 已投给其他 server
			// 或者已经投给自己
			r.msgs = append(r.msgs, *msgs)
			return
		}
		// 还一种可能:没有投票
	}

	if m.Term > r.Term {
		r.Term = m.Term
		r.Vote = 0
		r.becomeFollower(m.Term, m.From)
	}

	if lastLogTerm > m.LogTerm || (m.LogTerm == lastLogTerm && m.Index < lastLogIndex) {
		// 选取限制
		r.msgs = append(r.msgs, *msgs)
		fmt.Printf("lastLogTerm:%d m.LogTerm:%d\n", lastLogTerm, m.LogTerm)
		return
	}else{
	}

	// r.Term = m.Term
	r.Vote = m.From
	msgs.Reject = false

	// 选举失败，自己修改自己的Election时间
	// rf.changeRole(Follower)
	r.becomeFollower(m.Term, m.From)
	r.resetElectionTimeOut()
	r.msgs = append(r.msgs, *msgs)
	return
}

// wait for rewrite
func (r *Raft) rewrite_handleRequest(m pb.Message)  {
	// reply := pb.Message{}
	// reply.MsgType = pb.MessageType_MsgRequestVoteResponse  

	// if r.Term < m.Term{
	// 	r.Term = m.Term
	// 	reply.Reject = false
		
	// 	r.Vote = m.From



	// }else if r.Term == m.Term{
	// 	compare_log_term := m.LogTerm
	// 	cur_log_term, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	// 	if compare_log_term != cur_log_term{
	// 		if compare_log_term > cur_log_term{
	// 			if r.Vote == 0{
	// 				reply.Reject = false
	// 				r.Vote = m.From
	// 			}
	// 		}else if compare_log_term < cur_log_term{
	// 			reply.Reject = true
	// 		}else{
	// 			if r.RaftLog.LastIndex() < m.Index{
	// 				if r.Vote == 0{
	// 					reply.Reject = false
	// 					r.Vote = m.From
	// 				}
	// 			}else{
	// 				reply.Reject = true
	// 			}
	// 		}
	// 	}else{
	// 		if m.Index >= r.RaftLog.LastIndex(){
	// 			if r.Vote == 0{
	// 				reply.Reject = false
	// 				r.Vote = m.From
	// 			}
	// 		}else{
	// 			reply.Reject = true
	// 		}
	// 	}
	// 	// reply.Reject = true
	// }else{
	// 	reply.Reject = true
	// }

	// if r.Vote != m.From{
	// 	reply.Reject = true
	// }
	
	// reply.Term = r.Term
	// reply.From = r.id
	// reply.To = m.From
	// r.msgs = append(r.msgs, reply)
}