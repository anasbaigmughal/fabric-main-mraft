package mraft

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"time"
)

type Server interface {
	ServId() int

	PeersIds() []int

	GetTerm() uint64

	GetPresentLeader() int

	GetVotedForNode() int

	GetcommittedLength() uint64

	GetLastApplied() uint64

	Start()

	Outbox() chan *Envelope

	Inbox() chan *Envelope

	No_Of_Peers() int

	ClientServerComm(clientPortAddr string)

	RetLsn() Lsn

	GetState() int

	resetSelectTimeout()

	loop()

	follower()

	candidate()

	PresentLeader()

	GetPrevLogIndex() uint64

	GetPrevCandidateLogTerm() uint64

	SetTerm(uint64)

	SetVotedForNode(int)

	requestForVoteToPeers()

	handleRequestVote(env *Envelope) bool

	sendHeartBeats(ni *nextIndex, timeout time.Duration) (int, bool)

	handleAppendEntries(env *Envelope) (*appendEntriesResponse, bool)

	ApplyCommandToSM()
}

func (ServerVar *mRaft) Start() {

	ServerVar.loop()

}

func (ServerVar *mRaft) SetTerm(term uint64) {
	ServerVar.Term = term

}

func (ServerVar *mRaft) GetPrevLogIndex() uint64 {
	return (*ServerVar).PrevLogIndex
}

func (ServerVar *mRaft) GetPrevCandidateLogTerm() uint64 {
	return (*ServerVar).PrevCandidateLogTerm
}

func (ServerVar *mRaft) GetLastApplied() uint64 {
	return (*ServerVar).LastApplied
}

func (ServerVar *mRaft) GetState() int {
	return (*ServerVar).State
}

func (ServerVar *mRaft) GetcommittedLength() uint64 {
	return (*ServerVar).committedLength
}

func (ServerVar *mRaft) GetTerm() uint64 {
	return (*ServerVar).Term
}

func (ServerVar *mRaft) GetPresentLeader() int {
	return (*ServerVar).PresentLeaderId
}

func (ServerVar *mRaft) GetVotedForNode() int {
	return (*ServerVar).VotedForNode
}

func (ServerVar *mRaft) SetVotedForNode(vote int) {

	(*ServerVar).VotedForNode = vote
}

func (ServerVar *mRaft) RetLsn() Lsn {

	return Lsn((*ServerVar).LsnVar)
}

func (ServerVar *mRaft) No_Of_Peers() int {
	return (*ServerVar).ClusterSize
}

func (ServerVar *mRaft) Outbox() chan *Envelope {
	return (*ServerVar).Out
}

func (ServerVar *mRaft) Inbox() chan *Envelope {
	return (*ServerVar).In
}

func (ServerVar *mRaft) ServId() int {
	return (*ServerVar).Pid
}

func (ServerVar *mRaft) PeersIds() []int {
	return (*ServerVar).Peers
}

func (ServerVar *mRaft) loop() {

	for {

		state := ServerVar.GetState()

		switch state {
		case FOLLOWER:
			ServerVar.follower()

		case CANDIDATE:
			ServerVar.candidate()

		case PresentLeader:
			ServerVar.PresentLeader()

		default:
			return
		}
	}
}

func (ServerVar *mRaft) catchUpLog(ni *nextIndex, id int, timeout time.Duration) error {

	presentTerm := ServerVar.Term

	prevLogIndex := ni.prevLogIndex(uint64(id))

	resultentries, prevCandidateLogTerm := ServerVar.SLog.entriesAfter(prevLogIndex, 10)

	var envVar Envelope
	envVar.Pid = id
	envVar.MessageId = APPENDENTRIES
	envVar.SenderId = ServerVar.ServId()
	envVar.PresentLeaderid = ServerVar.PresentLeaderId
	envVar.LastLogIndex = prevLogIndex
	envVar.LastCandidateLogTerm = prevCandidateLogTerm
	envVar.committedLength = ServerVar.SLog.committedLength

	envVar.Message = &appendEntries{TermIndex: presentTerm, Entries: resultentries}
	if debug {
		fmt.Println("Server ", ServerVar.ServId(), "->", id, " Prev log= ", prevLogIndex, "Prev Term = ", prevCandidateLogTerm)
	}
	ServerVar.Outbox() <- &envVar

	select {

	case env := <-(ServerVar.Inbox()):
		switch env.MessageId {

		case APPENDENTRIESRESPONSE:

			id = env.SenderId
			prevLogIndex = ni.prevLogIndex(uint64(id))

			resultentries, prevCandidateLogTerm = ServerVar.SLog.entriesAfter(prevLogIndex, 10)

			if debug {

				fmt.Println("Received APPENDENTRIESRESPONSE at RIGHT place")
			}

			resp := env.Message.(appendEntriesResponse)
			if resp.Term > presentTerm {

				return errorDeposed
			}

			if !resp.Success {

				newPrevLogIndex, err := ni.decrement(uint64(id), prevLogIndex)
				if debug {
					fmt.Println("No SUCC: new index for ", id, "is", newPrevLogIndex, "where prev log index was ", prevLogIndex)
				}
				if err != nil {

					return err
				}
				if debug {
					fmt.Println("flush to %v: rejected")
				}
				return errorappendEntriesRejected
			}

			if len(resultentries) > 0 {
				newPrevLogIndex, err := ni.set(uint64(id), uint64(resultentries[len(resultentries)-1].Lsn()), prevLogIndex)
				if debug {
					fmt.Println("SET : new prev index for ", id, "is", newPrevLogIndex, "Term  = ", resp.Term)
				}
				if err != nil {

					return err
				}

				return nil
			}

			return nil
		}
	case <-time.After(2 * timeout):
		return errorTimeout
	}
	return nil
}

func (ServerVar *mRaft) handleAppendEntries(env *Envelope) (*appendEntriesResponse, bool) {

	if env == nil {

		fmt.Println("Eureka..............")
	}

	resp := env.Message.(appendEntries)
	if debug {
		fmt.Println("handleAppendEntries() : Fo server ", ServerVar.ServId(), " Term : ", ServerVar.Term, " env Term = ", resp.TermIndex, "Commit index ", env.committedLength, " ServerVar.SLog.committedLength ", ServerVar.SLog.committedLength)
	}

	if resp.TermIndex < ServerVar.Term {

		return &appendEntriesResponse{
			Term:    ServerVar.Term,
			Success: false,
			reason:  fmt.Sprintf("Term is less"),
		}, false
	}

	downGrade := false

	if resp.TermIndex > ServerVar.Term {
		ServerVar.Term = resp.TermIndex
		ServerVar.VotedForNode = NOVOTE
		downGrade = true

	}

	if ServerVar.State == CANDIDATE && env.SenderId != ServerVar.PresentLeaderId && resp.TermIndex >= ServerVar.Term {
		ServerVar.Term = resp.TermIndex
		ServerVar.VotedForNode = NOVOTE
		downGrade = true
	}
	ServerVar.resetSelectTimeout()

	if err := ServerVar.SLog.discardEntries(env.LastLogIndex, env.LastCandidateLogTerm); err != nil {

		return &appendEntriesResponse{
			Term:    ServerVar.Term,
			Success: false,
			reason:  fmt.Sprintf("while ensuring last log entry had index=%d term=%d: error: %s", env.LastLogIndex, env.LastCandidateLogTerm, err)}, downGrade
	}

	for i, entry := range resp.Entries {

		if err := ServerVar.SLog.appendEntry(entry); err != nil {

			return &appendEntriesResponse{
				Term:    ServerVar.Term,
				Success: false,
				reason: fmt.Sprintf(
					"AppendEntry %d/%d failed: %s",
					i+1,
					len(resp.Entries),
					err,
				),
			}, downGrade

		}
		if debug {
			fmt.Println("handle() Server ", ServerVar.ServId(), " appendEntry")
		}
	}

	if env.committedLength > 0 && env.committedLength > ServerVar.SLog.committedLength {

		if err := ServerVar.SLog.commitTill(env.committedLength); err != nil {

			return &appendEntriesResponse{
				Term:    ServerVar.Term,
				Success: false,
				reason:  fmt.Sprintf("CommitTo failed"),
			}, downGrade
		}

	}

	return &appendEntriesResponse{
		Term:    ServerVar.Term,
		Success: true,
	}, downGrade

}

func (ServerVar *mRaft) newNextIndex(defaultNextIndex uint64) *nextIndex {
	ni := &nextIndex{
		m: map[uint64]uint64{},
	}
	for _, id := range ServerVar.PeersIds() {
		ni.m[uint64(id)] = defaultNextIndex

	}
	return ni
}

func (ni *nextIndex) prevLogIndex(id uint64) uint64 {
	ni.RLock()
	defer ni.RUnlock()
	if _, ok := ni.m[id]; !ok {
		panic(fmt.Sprintf("peer %d not found", id))
	}
	return ni.m[id]
}

func (ni *nextIndex) decrement(id uint64, prev uint64) (uint64, error) {
	ni.Lock()
	defer ni.Unlock()
	i, ok := ni.m[id]
	if !ok {
		panic(fmt.Sprintf("peer %d not found", id))
	}
	if i != prev {
		return i, errorOutOfSync
	}
	if i > 0 {

		ni.m[id]--
		if debug {

			fmt.Println("decremented val ", ni.m[id])
		}
	}
	return ni.m[id], nil
}

func (ni *nextIndex) set(id, index, prev uint64) (uint64, error) {
	ni.Lock()
	defer ni.Unlock()
	i, ok := ni.m[id]
	if !ok {
		panic(fmt.Sprintf("server %d not found", id))
	}
	if i != prev {
		return i, errorOutOfSync
	}
	ni.m[id] = index

	if debug {
		fmt.Println("set val ", ni.m[id])
	}
	return index, nil
}

func (ServerVar *mRaft) sendHeartBeats(ni *nextIndex, timeout time.Duration) (int, bool) {

	type tuple struct {
		id  uint64
		err error
	}
	responses := make(chan tuple, len(ServerVar.PeersIds()))
	for _, id1 := range ServerVar.PeersIds() {
		go func(id int) {
			errChan := make(chan error, 1)
			go func() { errChan <- ServerVar.catchUpLog(ni, id, timeout) }()
			responses <- tuple{uint64(id), <-errChan}
		}(id1)
	}
	successes, downGrade := 0, false
	for i := 0; i < cap(responses); i++ {
		switch t := <-responses; t.err {
		case nil:
			successes++
		case errorDeposed:
			downGrade = true
		default:
		}
	}
	return successes, downGrade

}

func (ServerVar *mRaft) PresentLeader() {

	ReplicateLog := make(chan struct{})

	hbeat := time.NewTicker(heartBeatInterval())

	defer hbeat.Stop()
	go func() {
		for _ = range hbeat.C {
			ReplicateLog <- struct{}{}
		}
	}()

	nIndex := ServerVar.newNextIndex(uint64(ServerVar.SLog.lastIndex()))

	for {
		select {

		case t := <-ServerVar.Outchan:

			cmd := t.(*CommandTuple)

			if debug {
				fmt.Println("got command, appending", ServerVar.Term)

			}
			presentTerm := ServerVar.Term
			comma := new(bytes.Buffer)
			encCommand := gob.NewEncoder(comma)
			encCommand.Encode(cmd)
			entry := &LogEntryStruct{
				Logsn:     Lsn(ServerVar.SLog.lastIndex() + 1),
				TermIndex: presentTerm,
				DataArray: cmd.Com,
				Commit:    cmd.ComResponse,
			}

			if err := ServerVar.SLog.appendEntry(entry); err != nil {
				panic(err)
				continue
			}
			if debug {

				fmt.Printf(" PresentLeader after append, committedLength=%d lastIndex=%d recentTerm=%d", ServerVar.SLog.getcommittedLength(), ServerVar.SLog.lastIndex(), ServerVar.SLog.recentTerm())
			}
			go func() {

				ReplicateLog <- struct{}{}
			}()

		case <-ReplicateLog:

			successes, downGrade := ServerVar.sendHeartBeats(nIndex, 2*heartBeatInterval())

			if downGrade {

				ServerVar.PresentLeaderId = UNKNOWN
				ServerVar.State = FOLLOWER

				return
			}

			if successes >= Quorum-1 {

				var indices []uint64
				indices = append(indices, ServerVar.SLog.currentIndex())
				for _, i := range nIndex.m {
					indices = append(indices, i)
				}
				sort.Sort(uint64Slice(indices))

				committedLength := indices[Quorum-1]

				committedIndex := ServerVar.SLog.committedLength

				peersBestIndex := committedLength

				ourLastIndex := ServerVar.SLog.lastIndex()

				ourcommittedLength := ServerVar.SLog.getcommittedLength()

				if peersBestIndex > ourLastIndex {
					ServerVar.PresentLeaderId = UNKNOWN
					ServerVar.VotedForNode = NOVOTE
					ServerVar.State = FOLLOWER
					return
				}

				if committedLength > committedIndex {

					if err := ServerVar.SLog.commitTill(peersBestIndex); err != nil {

						continue
					}
					if ServerVar.SLog.getcommittedLength() > ourcommittedLength {
						go func() { ReplicateLog <- struct{}{} }()
					}
				}

			}

		case env := <-(ServerVar.Inbox()):
			switch env.MessageId {

			case REQUESTVOTE:

				if debug {
					fmt.Println("Received request vote for candidate....")
				}

				downgrade := ServerVar.handleRequestVote(env)

				if downgrade {

					ServerVar.PresentLeaderId = UNKNOWN
					ServerVar.State = FOLLOWER

					return
				}

			case VOTERESPONSEMSG:

			case APPENDENTRIES:
				if debug {
					fmt.Println("Received APPENDENTRIES for ", ServerVar.ServId())
				}
				resp, down := ServerVar.handleAppendEntries(env)

				var envVar Envelope
				envVar.Pid = env.PresentLeaderid
				envVar.MessageId = APPENDENTRIESRESPONSE
				envVar.SenderId = ServerVar.ServId()
				envVar.PresentLeaderid = ServerVar.PresentLeaderId
				envVar.LastLogIndex = env.LastLogIndex
				envVar.LastCandidateLogTerm = env.LastCandidateLogTerm
				envVar.committedLength = env.committedLength

				envVar.Message = resp

				ServerVar.Outbox() <- &envVar

				if down {
					ServerVar.PresentLeaderId = env.PresentLeaderid
					ServerVar.State = FOLLOWER
					return

				}

			}
		}

	}

}

func heartBeatInterval() time.Duration {
	tm := MinSelectTo / 6
	return time.Duration(tm) * time.Millisecond
}

func (ServerVar *mRaft) candidate() {

	ServerVar.requestForVoteToPeers()

	if debug {
		fmt.Println("CANDIDATE ID = ", ServerVar.ServId())
	}

	for {
		select {
		case <-ServerVar.SelectTicker:
			ServerVar.resetElectTimeout()
			ServerVar.Term++
			ServerVar.VotedForNode = NOVOTE

			if debug {
				fmt.Println("TIMEOUT for CANDIDATE ID = ", ServerVar.ServId(), "New Term = ", ServerVar.Term)
			}

			return

		case env := <-(ServerVar.Inbox()):

			if debug {
				fmt.Println("CANDIDATE : Received Message is %v for %d ", env.MessageId, ServerVar.ServId())
			}

			switch env.MessageId {

			case REQUESTVOTE:

				if debug {
					fmt.Println("Received request vote for candidate....")
				}

				downgrade := ServerVar.handleRequestVote(env)

				if downgrade {

					ServerVar.PresentLeaderId = UNKNOWN
					ServerVar.State = FOLLOWER
					return
				}

			case VOTERESPONSEMSG:
				les := env.Message.(LogEntryStruct)
				if les.TermIndex > ServerVar.Term {
					ServerVar.PresentLeaderId = UNKNOWN
					ServerVar.State = FOLLOWER
					ServerVar.VotedForNode = NOVOTE
					if debug {
						fmt.Println("Message term is greater for candidate = ", ServerVar.ServId(), " becoming follower")
					}

					return
				}

				if les.TermIndex < ServerVar.Term {
					break
				}

				voteLock.Lock()
				voteMap[env.SenderId] = true
				voteLock.Unlock()

				vcount := 1
				for i := range ServerVar.PeersIds() {
					if voteMap[ServerVar.Peers[i]] == true {
						vcount++
					}

				}
				if debug {
					fmt.Println(" Candidate Server id = ", ServerVar.ServId(), " vcount = ", vcount, " Quorum = ", Quorum)

				}

				if vcount >= (Quorum) {
					ServerVar.PresentLeaderId = ServerVar.ServId()
					ServerVar.State = PresentLeader
					ServerVar.VotedForNode = NOVOTE
					if debug {
						fmt.Println(" New PresentLeader Server id = ", ServerVar.ServId())

					}

					return
				}

			case APPENDENTRIES:
				if debug {
					fmt.Println("Received APPENDENTRIES for ", ServerVar.ServId())
				}
				resp, down := ServerVar.handleAppendEntries(env)

				var envVar Envelope
				envVar.Pid = env.PresentLeaderid
				envVar.MessageId = APPENDENTRIESRESPONSE
				envVar.SenderId = ServerVar.ServId()
				envVar.PresentLeaderid = ServerVar.PresentLeaderId
				envVar.LastLogIndex = env.LastLogIndex
				envVar.LastCandidateLogTerm = env.LastCandidateLogTerm
				envVar.committedLength = env.committedLength

				envVar.Message = resp
				ServerVar.Outbox() <- &envVar

				if down {
					ServerVar.PresentLeaderId = env.PresentLeaderid
					ServerVar.State = FOLLOWER
					return

				}

			}

		}

	}
}

func (ServerVar *mRaft) requestForVoteToPeers() {

	var lesn LogEntryStruct

	lesn.Logsn = Lsn((*ServerVar).LsnVar)
	lesn.DataArray = nil
	lesn.TermIndex = ServerVar.GetTerm()

	lesn.Commit = nil

	var envVar Envelope
	envVar.Pid = BROADCAST
	envVar.MessageId = REQUESTVOTE
	envVar.SenderId = ServerVar.ServId()
	envVar.LastLogIndex = ServerVar.GetPrevLogIndex()
	envVar.LastCandidateLogTerm = ServerVar.GetPrevCandidateLogTerm()
	envVar.Message = lesn

	ServerVar.VotedForNode = ServerVar.ServId()

	ServerVar.Outbox() <- &envVar

}

func (ServerVar *mRaft) follower() {

	if debug {
		fmt.Println(" Follower ID = ", ServerVar.ServId())
	}

	ServerVar.resetSelectTimeout()

	for {

		select {

		case <-ServerVar.SelectTicker:
			ServerVar.Term++
			ServerVar.VotedForNode = NOVOTE
			ServerVar.PresentLeaderId = UNKNOWN
			ServerVar.resetElectTimeout()
			ServerVar.State = CANDIDATE

			if debug {
				fmt.Println("TIMEOUT for Follower ID = ", ServerVar.ServId(), " Now Candidate")
			}
			return

		case env := <-(ServerVar.Inbox()):

			switch env.MessageId {

			case REQUESTVOTE:

				downgrade := ServerVar.handleRequestVote(env)

				if downgrade {

					ServerVar.PresentLeaderId = UNKNOWN
				}

			case VOTERESPONSEMSG:

			case APPENDENTRIES:

				if ServerVar.PresentLeaderId == UNKNOWN {
					ServerVar.PresentLeaderId = env.SenderId

				}

				resp, down := ServerVar.handleAppendEntries(env)

				var envVar Envelope
				envVar.Pid = env.PresentLeaderid
				envVar.MessageId = APPENDENTRIESRESPONSE
				envVar.SenderId = ServerVar.ServId()
				envVar.PresentLeaderid = ServerVar.PresentLeaderId
				envVar.LastLogIndex = env.LastLogIndex
				envVar.LastCandidateLogTerm = env.LastCandidateLogTerm
				envVar.committedLength = env.committedLength

				envVar.Message = resp

				ServerVar.Outbox() <- &envVar

				if down {
					ServerVar.PresentLeaderId = env.PresentLeaderid
					ServerVar.State = FOLLOWER
					return

				}

			}

		}
	}

}

func (serverVar *mRaft) handleRequestVote(req *Envelope) bool {

	resp := req.Message.(LogEntryStruct)

	if resp.TermIndex < serverVar.Term {
		return false
	}

	downgrade := false

	if resp.TermIndex > serverVar.Term {
		if debug {
			fmt.Println("RequestVote from newer term (%d): my term %d", resp.TermIndex, serverVar.Term)
		}
		serverVar.Term = resp.TermIndex
		serverVar.VotedForNode = NOVOTE
		serverVar.PresentLeaderId = UNKNOWN
		downgrade = true
	}

	if serverVar.GetState() == PresentLeader && !downgrade {

		return false
	}

	if serverVar.VotedForNode != NOVOTE && serverVar.VotedForNode != req.SenderId {

		return downgrade
	}

	if serverVar.PrevLogIndex > req.LastLogIndex || serverVar.PrevCandidateLogTerm > req.LastCandidateLogTerm {
		return downgrade
	}

	var lesn LogEntryStruct

	lesn.Logsn = Lsn((*serverVar).LsnVar)
	lesn.DataArray = nil
	lesn.TermIndex = serverVar.GetTerm()
	lesn.Commit = nil

	var envVar Envelope
	envVar.Pid = req.SenderId
	envVar.MessageId = VOTERESPONSEMSG
	envVar.SenderId = serverVar.ServId()
	envVar.LastLogIndex = serverVar.GetPrevLogIndex()
	envVar.LastCandidateLogTerm = serverVar.GetPrevCandidateLogTerm()
	envVar.Message = lesn

	if debug {

		fmt.Println("Sending vote for Candidate=", req.SenderId, " Term = ", lesn.TermIndex, " follower = ", envVar.SenderId)
	}

	serverVar.VotedForNode = req.SenderId
	serverVar.resetSelectTimeout()
	serverVar.Outbox() <- &envVar

	return downgrade
}

func FireAServer(myid int) Server {

	fileName := "clusterConfig.json"
	var obj ClusterConfig
	file, e := ioutil.ReadFile(fileName)

	if e != nil {
		panic("File error: " + e.Error())
	}

	json.Unmarshal(file, &obj)

	logfile := os.Getenv("GOPATH") + "/log/log_" + strconv.Itoa(myid)

	tLog := createNewLog(logfile)

	serverVar := &mRaft{
		Pid:                  UNKNOWN,
		Peers:                make([]int, len(obj.Servers)-1),
		Path:                 "",
		Term:                 INITIALIZATION,
		VotedForNode:         NOVOTE,
		VotedTerm:            UNKNOWN,
		PresentLeaderId:      UNKNOWN,
		committedLength:      0,
		SelectTicker:         nil,
		PrevLogIndex:         0,
		PrevCandidateLogTerm: 0,
		MatchIndex:           make(map[int]uint64),
		NextIndex:            make(map[int]uint64),
		LastApplied:          0,
		State:                FOLLOWER,
		In:                   make(chan *Envelope),
		Out:                  make(chan *Envelope),
		Address:              map[int]string{},
		ClientSockets:        make(map[int]*zmq.Socket),
		LsnVar:               0,
		LogSockets:           make(map[int]*zmq.Socket),
		Inchan:               make(chan *LogEntryStruct),
		Outchan:              make(chan interface{}),
		ClusterSize:          len(obj.Servers) - 1,
		GotConsensus:         make(chan bool),
		SLog:                 tLog,
		Inprocess:            false,
	}

	count := 0
	PresentLeaderID = UNKNOWN
	Quorum = UNKNOWN

	var clientPortAddr string
	for i := range obj.Servers {

		if obj.Servers[i].Id == strconv.Itoa(myid) {
			serverVar.Pid, _ = strconv.Atoi(obj.Servers[i].Id)
			clientPortAddr = obj.Servers[i].HostName + ":" + obj.Servers[i].ClientPort

		} else {

			serverVar.Peers[count], _ = strconv.Atoi(obj.Servers[i].Id)

			count++

		}

		if PresentLeaderID == -1 {
			PresentLeaderID, _ = strconv.Atoi(obj.Servers[i].Id)

		}

		serverVar.Path = obj.Path.Path

		servid, _ := strconv.Atoi(obj.Servers[i].Id)
		serverVar.Address[servid] = obj.Servers[i].HostName + ":" + obj.Servers[i].LogPort

	}

	gob.Register(LogEntryStruct{})
	gob.Register(appendEntries{})
	gob.Register(appendEntriesResponse{})
	gob.Register(Command{})

	no_servers, _ := strconv.Atoi(obj.Count.Count)
	Quorum = int((no_servers-1)/2 + 1.0)
	for i := range serverVar.PeersIds() {
		serverVar.LogSockets[serverVar.Peers[i]], _ = zmq.NewSocket(zmq.PUSH)
		serverVar.LogSockets[serverVar.Peers[i]].SetSndtimeo(time.Millisecond * 30)

		if err != nil {
			panic("Connect error " + err.Error())
		}

		serverVar.MatchIndex[serverVar.Peers[i]] = 0
		serverVar.NextIndex[serverVar.Peers[i]] = serverVar.GetLastApplied() + 1

		voteMap[serverVar.Peers[i]] = false

	}

	serverVar.SLog.ApplyFunc = func(e *LogEntryStruct) {

		serverVar.Inchan <- e
	}
	go SendMail(serverVar)
	go GetMail(serverVar)

	go serverVar.ClientServerComm(clientPortAddr)

	return serverVar
}

func SendMail(serverVar *mRaft) {
	var network bytes.Buffer
	for {
		envelope := <-(serverVar.Outbox())

		if envelope.Pid == BROADCAST {
			envelope.Pid = serverVar.ServId()
			for i := range serverVar.PeersIds() {
				network.Reset()
				enc := gob.NewEncoder(&network)
				err := enc.Encode(envelope)
				if err != nil {
					panic("gob error: " + err.Error())
				}

				serverVar.LogSockets[serverVar.Peers[i]].Send(network.String(), 0)
			}

		} else {

			network.Reset()
			a := envelope.Pid
			envelope.Pid = serverVar.ServId()
			enc := gob.NewEncoder(&network)
			err := enc.Encode(envelope)
			if err != nil {
				panic("gob error: " + err.Error())
			}

			serverVar.LogSockets[a].Send(network.String(), 0)

		}

	}
}

func GetMail(ServerVar *mRaft) {

	input, err := zmq.NewSocket(zmq.PULL)
	if err != nil {
		panic("Socket: " + err.Error())
	}
	if err != nil {
		panic("Socket: " + err.Error())
	}
	for {
		msg, err := input.Recv(0)

		if err != nil {
		}

		b := bytes.NewBufferString(msg)
		dec := gob.NewDecoder(b)

		env := new(Envelope)
		err = dec.Decode(env)
		if err != nil {
			log.Fatal("decode:", err)
		}

		(ServerVar.Inbox()) <- env

	}
}

func (ServerVar *mRaft) resetElectTimeout() {
	ServerVar.SelectTicker = time.NewTimer(selectTimeout()).C
}

func selectTimeout() time.Duration {
	min := rand.Intn(int(MaxSelectTo - MinSelectTo))
	tm := int(MinSelectTo) + min
	return time.Duration(tm) * time.Millisecond
}
