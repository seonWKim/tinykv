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
	"math/rand"

	"github.com/pingcap-incubator/tinykv/log"
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
	// randomizedTimeout of election interval
	randomizedTimeout int
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
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}

	lastIndex, err := c.Storage.LastIndex()
	if err != nil {
		log.Error("Error retrieving the last index %v", err)
		return nil
	}
	prs := make(map[uint64]*Progress)
	for _, id := range c.peers {
		prs[uint64(id)] = &Progress{
			// Match represents the index of the highest log entry known to be replicated on a follower.
			// Initializing it to 0 is corerct because, at the start, the leader has not confirmed any log replication with its followers
			Match: 0,

			// Index of the next entry the leader will send to a follower.
			// It should be initialized to the leader's last log entry + 1
			// Even if the Next is not the correct value, Raft can auto correct it
			Next: lastIndex,
		}
	}

	initialState, _, err := c.Storage.InitialState()
	if err != nil {
		log.Errorf("Error retrieving initial state: %v", err)
		return nil
	}

	return &Raft{
		id: c.ID,

		Term: initialState.Term,
		RaftLog: &RaftLog{
			storage: c.Storage,

			committed: initialState.Commit,
			applied:   c.Applied,
			stabled:   c.Applied,

			entries:         []pb.Entry{},
			pendingSnapshot: nil,
		},

		Prs:   prs,
		State: StateFollower,
		votes: make(map[uint64]bool),
		msgs:  make([]pb.Message, 0),

		Lead: None,

		heartbeatTimeout:  c.HeartbeatTick,
		electionTimeout:   c.ElectionTick,
		randomizedTimeout: c.ElectionTick + rand.Intn(c.ElectionTick),

		heartbeatElapsed: 0,
		electionElapsed:  0,

		leadTransferee: 0,

		PendingConfIndex: 0,
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	next := r.Prs[to].Next
	var entries []pb.Entry
	if next < uint64(len(r.RaftLog.entries)) {
		entries = r.RaftLog.entries[next:]
	}

	entryPtrs := []*pb.Entry{}
	for _, entry := range entries {
		entryPtrs = append(entryPtrs, &entry)
	}

	prevLogIndex := next - 1
	prevLogTerm := uint64(0)
	if prevLogIndex > 0 && prevLogIndex < uint64(len(r.RaftLog.entries)) {
		prevLogTerm = r.RaftLog.entries[prevLogIndex].Term
	}

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: prevLogTerm,
		Index:   prevLogIndex,
		Entries: entryPtrs,
		Commit:  r.RaftLog.committed,
	})

	return true
}

func (r *Raft) sendHeartbeatToAll() {
	// Let's reset heartbeatElapsed right before sending heartbeat
	for id := range r.Prs {
		if r.id == id {
			// Because Prs contains information for all nodes, let's skip the leader's id
			continue
		}

		r.sendHeartbeat(id)
	}
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	var commit uint64
	if r.RaftLog != nil {
		commit = r.RaftLog.committed
	}

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  commit,
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	switch r.State {
	case StateLeader:
		r.heartbeatElapsed += 1
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgBeat,
				Term:    r.Term,
			})

			r.heartbeatElapsed = 0
		}

	case StateCandidate, StateFollower:
		r.electionElapsed += 1
		if r.electionElapsed >= r.randomizedTimeout {
			// initialize state for vote
			r.votes = make(map[uint64]bool)

			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
				Term:    r.Term,
			})

			r.randomizedTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
			r.electionElapsed = 0
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	if r.Term > term {
		log.Debugf("Follower's term(%v) is higher than the leader's(%v)", r.Term, term)
		return
	}

	r.Term = term
	r.Lead = lead
	r.Vote = None
	r.State = StateFollower
}

func (r *Raft) becomeCandidate() {
	r.Term += 1
	r.votes[r.id] = true
	// TODO: check whether placing this logic here is appropriate
	if len(r.Prs) <= 1 {
		r.becomeLeader()
	} else {
		r.State = StateCandidate
	}
}

func (r *Raft) sendRequestVoteToAll() {
	for id := range r.Prs {
		if r.id == id {
			continue
		}

		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			To:      id,
			From:    r.id,
			Term:    r.Term,
		})
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	r.State = StateLeader
	r.Lead = r.id

	// NOTE: This will cause the leader to append no-op entry to its log and then replicate it to its followers
	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{{}},
	})
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		r.handleFollowerMessage(m)
	case StateCandidate:
		r.handleCandidateMessage(m)
	case StateLeader:
		r.handleLeaderMessage(m)
	}
	return nil
}

// FollowerMessageHandler defines the interface for handling messages in follower state
type FollowerMessageHandler interface {
	Handle(r *Raft, m pb.Message) error
}

// FollowerRequestVoteHandler handles vote request messages for follower
type FollowerRequestVoteHandler struct{}

func (h *FollowerRequestVoteHandler) Handle(r *Raft, m pb.Message) error {
	// TODO: fix this later
	isLogUpToDate := true
	canVote := r.Vote == None || r.Vote == m.From
	reject := m.Term < r.Term || !canVote || !isLogUpToDate

	if !reject {
		r.Vote = m.From
		r.electionElapsed = 0
	}
	log.Debugf("Node(%v) received MsgRequestVote from Node(%v). Rejected: %v", r.id, m.From, reject)

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
	})
	return nil
}

// FollowerAppendHandler handles append messages for follower
type FollowerAppendHandler struct{}

func (h *FollowerAppendHandler) Handle(r *Raft, m pb.Message) error {
	r.electionElapsed = 0
	// Raft followers are passive and only learn the leader's identity through AppendEntries and HeartBeat messages
	r.Lead = m.From
	return nil
}

// FollowerHeartbeatHandler handles heartbeat messages for follower
type FollowerHeartbeatHandler struct{}

func (h *FollowerHeartbeatHandler) Handle(r *Raft, m pb.Message) error {
	r.electionElapsed = 0
	// Raft followers are passive and only learn the leader's identity through AppendEntries and HeartBeat messages
	r.Lead = m.From
	r.handleHeartbeat(m)
	return nil
}

// FollowerHupHandler handles hup (election timeout) messages for follower
type FollowerHupHandler struct{}

func (h *FollowerHupHandler) Handle(r *Raft, m pb.Message) error {
	// Don't have to reset electionElapsed because tick() will reset it
	r.becomeCandidate()
	r.sendRequestVoteToAll()
	return nil
}

// FollowerMessageProcessor manages follower message handling
type FollowerMessageProcessor struct {
	handlers map[pb.MessageType]FollowerMessageHandler
}

func NewFollowerMessageProcessor() *FollowerMessageProcessor {
	return &FollowerMessageProcessor{
		handlers: map[pb.MessageType]FollowerMessageHandler{
			pb.MessageType_MsgRequestVote: &FollowerRequestVoteHandler{},
			pb.MessageType_MsgAppend:      &FollowerAppendHandler{},
			pb.MessageType_MsgHeartbeat:   &FollowerHeartbeatHandler{},
			pb.MessageType_MsgHup:         &FollowerHupHandler{},
		},
	}
}

func (p *FollowerMessageProcessor) Process(r *Raft, m pb.Message) error {
	if handler, exists := p.handlers[m.MsgType]; exists {
		return handler.Handle(r, m)
	}
	return nil
}

// Package-level singleton for efficient reuse
var followerMessageProcessor = NewFollowerMessageProcessor()

// TODO: should we reset electionElapsed for every messages or just heartbeat message?
func (r *Raft) handleFollowerMessage(m pb.Message) error {
	if m.Term > r.Term {
		// The message might be from the candidate. So the lead should be None
		r.becomeFollower(m.Term, None)
	}

	return followerMessageProcessor.Process(r, m)
}

// CandidateMessageHandler defines the interface for handling messages in candidate state
type CandidateMessageHandler interface {
	Handle(r *Raft, m pb.Message) error
}

// CandidateAppendHandler handles append messages for candidate
type CandidateAppendHandler struct{}

func (h *CandidateAppendHandler) Handle(r *Raft, m pb.Message) error {
	r.electionElapsed = 0
	r.becomeFollower(m.Term, m.From)
	return nil
}

// CandidateHeartbeatHandler handles heartbeat messages for candidate
type CandidateHeartbeatHandler struct{}

func (h *CandidateHeartbeatHandler) Handle(r *Raft, m pb.Message) error {
	r.electionElapsed = 0
	r.becomeFollower(m.Term, m.From)
	return nil
}

// CandidateHupHandler handles hup (election timeout) messages for candidate
type CandidateHupHandler struct{}

func (h *CandidateHupHandler) Handle(r *Raft, m pb.Message) error {
	// Don't have to reset electionElapsed because tick() will reset it
	r.becomeCandidate()
	r.sendRequestVoteToAll()
	return nil
}

// CandidateRequestVoteHandler handles vote request messages for candidate
type CandidateRequestVoteHandler struct{}

func (h *CandidateRequestVoteHandler) Handle(r *Raft, m pb.Message) error {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		return r.Step(m)
	}

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  true,
	})
	return nil
}

// CandidateRequestVoteResponseHandler handles vote response messages for candidate
type CandidateRequestVoteResponseHandler struct{}

func (h *CandidateRequestVoteResponseHandler) Handle(r *Raft, m pb.Message) error {
	if m.Term != r.Term {
		return nil
	}

	r.votes[m.From] = !m.Reject
	log.Debugf("Node(%v) received MsgRequestVoteResponse from Node(%v), rejected: %v", r.id, m.From, m.Reject)
	if !m.Reject {
		r.checkAndBecomeLeader()
	}
	return nil
}

// CandidateMessageProcessor manages candidate message handling
type CandidateMessageProcessor struct {
	handlers map[pb.MessageType]CandidateMessageHandler
}

func NewCandidateMessageProcessor() *CandidateMessageProcessor {
	return &CandidateMessageProcessor{
		handlers: map[pb.MessageType]CandidateMessageHandler{
			pb.MessageType_MsgAppend:              &CandidateAppendHandler{},
			pb.MessageType_MsgHeartbeat:           &CandidateHeartbeatHandler{},
			pb.MessageType_MsgHup:                 &CandidateHupHandler{},
			pb.MessageType_MsgRequestVote:         &CandidateRequestVoteHandler{},
			pb.MessageType_MsgRequestVoteResponse: &CandidateRequestVoteResponseHandler{},
		},
	}
}

func (p *CandidateMessageProcessor) Process(r *Raft, m pb.Message) error {
	// Check term before processing (except for MsgHup)
	if uint64(m.MsgType) != uint64(pb.MessageType_MsgHup) && r.Term > m.Term {
		log.Debugf("Candidate's term(%v) is higher than the message's term(%v)", r.Term, m.Term)
		return nil
	}

	if handler, exists := p.handlers[m.MsgType]; exists {
		return handler.Handle(r, m)
	}
	return nil
}

// Package-level singleton for efficient reuse
var candidateMessageProcessor = NewCandidateMessageProcessor()

func (r *Raft) handleCandidateMessage(m pb.Message) error {
	return candidateMessageProcessor.Process(r, m)
}

// LeaderMessageHandler defines the interface for handling messages in leader state
type LeaderMessageHandler interface {
	Handle(r *Raft, m pb.Message) error
}

// RequestVoteHandler handles vote requests for leader
type RequestVoteHandler struct{}

func (h *RequestVoteHandler) Handle(r *Raft, m pb.Message) error {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		return r.Step(m)
	}

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  true,
	})
	return nil
}

// AppendHandler handles append messages for leader
type AppendHandler struct{}

func (h *AppendHandler) Handle(r *Raft, m pb.Message) error {
	if r.Term > m.Term {
		log.Debugf("How dare you send message with term(%v), I'm the leader with term %v", m.Term, r.Term)
	} else {
		log.Debugf("A new leader with term %v. Should I(term=%v) fall back to follower?", m.Term, r.Term)
		r.becomeFollower(m.Term, m.From)
	}
	return nil
}

// ProposeHandler handles propose messages for leader
type ProposeHandler struct{}

func (h *ProposeHandler) Handle(r *Raft, m pb.Message) error {
	lastIndex := r.RaftLog.LastIndex()
	for i, entry := range m.Entries {
		entry.Term = r.Term
		entry.Index = lastIndex + 1 + uint64(i)
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}

	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1

	for to := range r.Prs {
		if r.id == to {
			continue
		}

		r.sendAppend(to)
	}

	return nil
}

// BeatHandler handles beat messages for leader
type BeatHandler struct{}

func (h *BeatHandler) Handle(r *Raft, m pb.Message) error {
	r.sendHeartbeatToAll()
	return nil
}

// LeaderMessageProcessor manages leader message handling
type LeaderMessageProcessor struct {
	handlers map[pb.MessageType]LeaderMessageHandler
}

func NewLeaderMessageProcessor() *LeaderMessageProcessor {
	return &LeaderMessageProcessor{
		handlers: map[pb.MessageType]LeaderMessageHandler{
			pb.MessageType_MsgRequestVote: &RequestVoteHandler{},
			pb.MessageType_MsgAppend:      &AppendHandler{},
			pb.MessageType_MsgPropose:     &ProposeHandler{},
			pb.MessageType_MsgBeat:        &BeatHandler{},
		},
	}
}

func (p *LeaderMessageProcessor) Process(r *Raft, m pb.Message) error {
	if handler, exists := p.handlers[m.MsgType]; exists {
		return handler.Handle(r, m)
	}
	return nil
}

// Package-level singleton for efficient reuse
var leaderMessageProcessor = NewLeaderMessageProcessor()

func (r *Raft) handleLeaderMessage(m pb.Message) error {
	return leaderMessageProcessor.Process(r, m)
}

func (r *Raft) checkAndBecomeLeader() {
	count := 0
	totalNodesCount := len(r.Prs)
	for _, v := range r.votes {
		if v {
			count++
		}

		if count > totalNodesCount/2 {
			log.Debugf("Node(%v) is now leader!!", r.id)
			r.becomeLeader()
			break
		}
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

func (r *Raft) handleHeartbeat(m pb.Message) {
	if m.MsgType != pb.MessageType_MsgHeartbeat {
		log.Fatal("Not heartbeat message")
	}

	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
	})
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
