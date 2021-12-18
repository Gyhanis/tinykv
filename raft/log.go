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

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	lastIndex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	li, err := storage.LastIndex()
	if err != nil {
		return nil
	}
	fi, err := storage.FirstIndex()
	if err != nil {
		return nil
	}
	ents, err := storage.Entries(fi, li+1)
	if err != nil {
		return nil
	}
	hs, _, err := storage.InitialState()
	if err != nil {
		return nil
	}
	log := &RaftLog{
		storage:         storage,
		committed:       hs.Commit,
		applied:         0,
		stabled:         li,
		entries:         ents,
		pendingSnapshot: nil,

		lastIndex: li,
	}
	return log
}

func (l *RaftLog) truncate(end uint64) {
	i, _ := l.locate(end, 0, int64(len(l.entries)-1))
	l.entries = l.entries[:i+1]
	l.lastIndex = i
}

func (l *RaftLog) log(ents []*pb.Entry) {
	for _, ent := range ents {
		// fmt.Printf("Appending %v\n", ent)
		if ent.Index > l.lastIndex {
			l.entries = append(l.entries, *ent)
			l.lastIndex = ent.Index
		} else {
			i, _ := l.locate(ent.Index, 0, int64(len(l.entries)-1))
			if l.entries[i].Term != ent.Term {
				l.entries = l.entries[:i]
				l.entries = append(l.entries, *ent)
				l.lastIndex = ent.Index
				l.stabled = min(l.stabled, i)
			}
		}
	}
}

func (l *RaftLog) commit(i uint64) {
	l.committed = max(l.committed, i)
}

func (l *RaftLog) locate(i uint64, low, high int64) (uint64, error) {
	if high < low {
		return 0, errors.New("Entry with expected Index not found.")
	}
	j := (high + low) / 2
	for l.entries[j].Index != i {
		if l.entries[j].Index > i {
			high = j - 1
		} else {
			low = j + 1
		}
		if high < low {
			return 0, errors.New("Entry with expected Index not found.")
		}
		j = (high + low) / 2
	}
	return uint64(j), nil
}

func (l *RaftLog) getEntries(i, j uint64) ([]pb.Entry, error) {
	ents := make([]pb.Entry, 0)
	// if i < l.entries[0].Index {
	// 	if j < l.entries[0].Index {
	// 		return l.storage.Entries(i, j)
	// 	} else {
	// 		li, err := l.storage.LastIndex()
	// 		if err != nil {
	// 			return nil, err
	// 		}
	// 		tmp, err := l.storage.Entries(i, li+1)
	// 		if err != nil {
	// 			return nil, err
	// 		}
	// 		k, err := l.locate(j, 0, int64(len(l.entries)-1))
	// 		if err != nil {
	// 			return nil, err
	// 		}
	// 		tmp = append(tmp, l.entries[:k+1]...)
	// 		return tmp, nil
	// 	}
	// } else {
	m, err := l.locate(i, 0, int64(len(l.entries)-1))
	if err != nil {
		return ents, err
	}
	n, err := l.locate(j, 0, int64(len(l.entries)-1))
	if err != nil {
		return ents, err
	}
	ents = append(ents, l.entries[m:n+1]...)
	return ents, nil
	// }
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	ents, _ := l.getEntries(l.stabled+1, l.lastIndex)
	return ents
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	ents, _ = l.getEntries(l.applied+1, l.committed)
	return ents
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	return l.lastIndex
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	li, _ := l.storage.LastIndex()
	if i <= li {
		return l.storage.Term(i)
	}
	high := int64(len(l.entries) - 1)
	low := int64(0)
	j, err := l.locate(i, low, high)
	if err != nil {
		return 0, nil
	}
	return l.entries[j].Term, nil
}
