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
	"time"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
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
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
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

	// My fields
	gen              rand.Rand
	electionTimeoutR int
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	raft := Raft{}
	raft.Term = 0
	raft.electionElapsed = 0
	raft.electionTimeout = c.ElectionTick
	raft.gen = *rand.New(rand.NewSource(time.Now().UnixNano()))
	raft.electionTimeoutR = raft.electionTimeout + raft.gen.Intn(raft.electionTimeout)
	raft.heartbeatElapsed = 0
	raft.heartbeatTimeout = c.HeartbeatTick
	raft.id = c.ID
	raft.votes = make(map[uint64]bool)
	for _, peer := range c.peers {
		raft.votes[peer] = false
	}
	raft.Prs = make(map[uint64]*Progress)
	for _, peer := range c.peers {
		if peer != raft.id {
			raft.Prs[peer] = &Progress{
				Match: 0,
				Next:  0,
			}
		}
	}
	raft.msgs = make([]pb.Message, 0, 4)
	raft.Lead = 0
	raft.PendingConfIndex = 0
	raft.RaftLog = newLog(c.Storage)
	raft.State = StateFollower
	raft.Term = 0
	return &raft
}

func (r *Raft) send(m pb.Message, to uint64) {
	m.To = to
	r.msgs = append(r.msgs, m)
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return false
}

func (r *Raft) boardcast(m pb.Message) {
	for peer, _ := range r.votes {
		if peer != r.id {
			m.To = peer
			r.msgs = append(r.msgs, m)
		}
	}
}

func (r *Raft) newMessage(msg_type pb.MessageType) pb.Message {
	m := pb.Message{
		MsgType: msg_type,
		// To
		From:    r.id,
		Term:    r.Term,
		LogTerm: 0,
		Index:   0,
		// Entries:              make([]*pb.Entry, entry_size),
		Commit:   0,
		Snapshot: nil,
		Reject:   false,
		// XXX_NoUnkeyedLiteral: struct{}{},
		// XXX_unrecognized:     make([]byte, 0),
		XXX_sizecache: 0,
	}
	return m
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
// func (r *Raft) sendHeartbeat(to uint64) {
func (r *Raft) sendHeartbeat() {
	// Your Code Here (2A).
	m := r.newMessage(pb.MessageType_MsgHeartbeat)
	r.boardcast(m)
	// r.send(m, to)
}

func (r *Raft) requestVote() {
	m := r.newMessage(pb.MessageType_MsgRequestVote)
	r.boardcast(m)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			m := r.newMessage(pb.MessageType_MsgBeat)
			r.Step(m)
		}
	} else {
		r.electionElapsed++

		if r.electionElapsed >= r.electionTimeoutR {
			r.electionElapsed = 0
			m := r.newMessage(pb.MessageType_MsgHup)
			r.Step(m)
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Vote = 0
	r.Lead = lead
	r.electionElapsed = 0
	r.electionTimeoutR = r.electionTimeout + r.gen.Intn(r.electionTimeout)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term++
	r.Vote = r.id
	r.electionElapsed = 0
	r.electionTimeoutR = r.electionTimeout + r.gen.Intn(r.electionTimeout)
	for peer := range r.votes {
		r.votes[peer] = false
	}
	r.votes[r.id] = true
	r.checkVotes()
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Vote = 0
	r.Lead = r.id
	r.electionElapsed = 0
	r.electionTimeoutR = r.electionTimeout + r.gen.Intn(r.electionTimeout)
	r.heartbeatElapsed = 0
}

func (r *Raft) checkVotes() {
	sum := 0
	for _, vote := range r.votes {
		if vote {
			sum++
		}
	}
	if sum > (len(r.votes) / 2) {
		r.becomeLeader()
	}
}

func (r *Raft) vote(m pb.Message) {
	resp := r.newMessage(pb.MessageType_MsgRequestVoteResponse)
	if r.Vote == 0 || r.Vote == m.From {
		r.Vote = m.From
		resp.Reject = false
	} else {
		resp.Reject = true
	}
	r.send(resp, m.From)
}

func (r *Raft) tryCommit(i uint64) {
	if i > r.RaftLog.committed {
		sum := 0
		for _, progress := range r.Prs {
			if progress.Match > r.RaftLog.committed {
				sum++
			}
		}
		if sum >= (len(r.Prs) / 2) {
			r.RaftLog.commit(i)
		}
	}
}

var verbose bool = true

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if verbose {
		fmt.Printf("%v: Received message: %v\n", r.id, m)
	}
	if r.Term > m.Term {
		if m.MsgType == pb.MessageType_MsgHup {
			fmt.Printf("%v: MsgHup detected, ignoring Term(required by testing program)\n",
				r.id)
		} else if m.MsgType == pb.MessageType_MsgBeat {
			fmt.Printf("%v: MsgBeat detected, ignoring Term(required by testing program)\n",
				r.id)
		} else if m.MsgType == pb.MessageType_MsgPropose {
			fmt.Printf("%v: MsgPropose detected, ignoring Term(required by testing program)\n",
				r.id)
		} else {
			fmt.Printf("%v: Message with invalid term %v, current term %v\n",
				r.id, m.Term, r.Term)
			return nil
		}
	} else if r.Term < m.Term {
		r.becomeFollower(m.Term, m.From)
	}
	switch r.State {
	case StateFollower:
		r.Term = max(r.Term, m.Term)
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.becomeCandidate()
			r.requestVote()
		case pb.MessageType_MsgRequestVote:
			r.vote(m)
		case pb.MessageType_MsgHeartbeat:
			r.electionElapsed = 0
		case pb.MessageType_MsgAppend:
			r.RaftLog.log(m.Entries)
			commit := min(m.Entries[len(m.Entries)-1].Index, m.Commit)
			r.RaftLog.commit(commit)
			resp := r.newMessage(pb.MessageType_MsgAppendResponse)
			resp.LogTerm = m.LogTerm
			resp.Commit = commit
			resp.Reject = false
			r.send(resp, m.From)
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.becomeCandidate()
			r.requestVote()
		case pb.MessageType_MsgRequestVoteResponse:
			if !m.Reject {
				r.votes[m.From] = true
			}
			r.checkVotes()
		case pb.MessageType_MsgAppend:
			if m.Term >= r.Term {
				r.becomeFollower(m.Term, m.From)
				r.electionElapsed = 0
			}
		case pb.MessageType_MsgHeartbeat:
			if m.Term >= r.Term {
				r.becomeFollower(m.Term, m.From)
				r.electionElapsed = 0
			}

		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgBeat:
			r.sendHeartbeat()
		case pb.MessageType_MsgPropose:
			base := r.RaftLog.LastIndex() + 1
			for i, _ := range m.Entries {
				m.Entries[i].Term = r.Term
				m.Entries[i].Index = base + uint64(i)
			}
			r.RaftLog.log(m.Entries)
			if len(r.Prs) == 0 {
				r.tryCommit(m.Entries[len(m.Entries)-1].Index)
			} else {
				msg := r.newMessage(pb.MessageType_MsgAppend)
				msg.Entries = m.Entries
				msg.LogTerm = r.Term
				r.boardcast(msg)
			}
		case pb.MessageType_MsgAppendResponse:
			if !m.Reject {
				r.Prs[m.From].Match = m.Index
				r.Prs[m.From].Next = m.Index
				r.tryCommit(m.Index)
			}
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
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
