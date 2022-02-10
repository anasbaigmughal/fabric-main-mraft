package mraft

import (
	"fmt"
	"strconv"

	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"

	"github.com/syndtr/goleveldb/leveldb"
)

func createNewLog(dbPath string) *Log {
	db, err := leveldb.OpenFile(dbPath, nil)
	if err != nil {
		panic(fmt.Sprintf("No such directory,%v", err))
	}
	l := &Log{
		entries:     []*LogEntryStruct{},
		db:          db,
		commitIndex: 0,
		initialTerm: 0,
	}

	iter := l.db.NewIterator(nil, nil)
	count := 0

	for iter.Next() {
		count++
		entry := new(LogEntryStruct)
		value := iter.Value()
		b := bytes.NewBufferString(string(value))
		dec := gob.NewDecoder(b)
		err := dec.Decode(entry)
		if err != nil {
			panic(fmt.Sprintf("decode:", err))
		}
		if uint64(entry.Logsn) > 0 {

			l.entries = append(l.entries, entry)
			if uint64(entry.Logsn) <= l.commitIndex {
				l.ApplyFunc(entry)
			}
		}
	}
	iter.Release()
	return l
}

func (l *Log) currentIndex() uint64 {
	l.RLock()
	defer l.RUnlock()
	return l.CurrentIndexWithOutLock()
}

func (l *Log) CurrentIndexWithOutLock() uint64 {
	if len(l.entries) == 0 {
		return 0
	}
	return uint64(l.entries[len(l.entries)-1].Logsn)
}

func (l *Log) close() {
	l.Lock()
	defer l.Unlock()
	l.db.Close()
	l.entries = make([]*LogEntryStruct, 0)
}

func (l *Log) containsEntry(index uint64, term uint64) bool {
	entry := l.getEntry(index)
	return (entry != nil && uint64(entry.TermIndex) == term)
}

func (l *Log) getEntry(index uint64) *LogEntryStruct {
	l.RLock()
	defer l.RUnlock()
	if index <= 0 || index > (uint64(len(l.entries))) {
		return nil
	}
	return l.entries[index-1]
}

func (l *Log) entriesAfter(index uint64, maxLogEntriesPerRequest uint64) ([]*LogEntryStruct, uint64) {
	l.RLock()
	defer l.RUnlock()
	if index < 0 {
		return nil, 0
	}
	if index > (uint64(len(l.entries))) {
		panic(fmt.Sprintf("MRAFT: END OF LOG REACHED: %v %v", len(l.entries), index))
	}
	pos := 0
	lastTerm := uint64(0)

	for ; pos < len(l.entries); pos++ {
		if uint64(l.entries[pos].Logsn) > index {
			break
		}
		lastTerm = uint64(l.entries[pos].TermIndex)
	}
	a := l.entries[pos:]
	if len(a) == 0 {

		return []*LogEntryStruct{}, lastTerm
	}

	if uint64(len(a)) < maxLogEntriesPerRequest {
		return closeResponseChannels(a), lastTerm
	} else {

		return a[:maxLogEntriesPerRequest], lastTerm
	}
}

func closeResponseChannels(a []*LogEntryStruct) []*LogEntryStruct {
	stripped := make([]*LogEntryStruct, len(a))
	for i, entry := range a {
		stripped[i] = &LogEntryStruct{
			Logsn:     entry.Logsn,
			TermIndex: entry.TermIndex,
			DataArray: entry.DataArray,
			Commit:    nil,
		}
	}
	return stripped
}

func (l *Log) lastTerm() uint64 {
	l.RLock()
	defer l.RUnlock()
	if len(l.entries) <= 0 {
		return 0
	}
	return uint64(l.entries[len(l.entries)-1].TermIndex)
}

func (l *Log) lastTermWithOutLock() uint64 {
	if len(l.entries) <= 0 {
		return 0
	}
	return uint64(l.entries[len(l.entries)-1].TermIndex)
}

func (l *Log) discardEntries(index, term uint64) error {
	l.Lock()
	defer l.Unlock()

	if index > l.lastIndexWithOutLock() {
		return errIndexIsBig
	}
	if debug {
		fmt.Println("ERROR ", index, "  ", l.getCommitIndexWithOutLock())
	}
	if index < l.getCommitIndexWithOutLock() {

		return errIndexIsSmall
	}
	if index == 0 {
		for pos := 0; pos < len(l.entries); pos++ {
			if l.entries[pos].Commit != nil {
				l.entries[pos].Commit <- false
				close(l.entries[pos].Commit)
				l.entries[pos].Commit = nil
			}
		}
		l.entries = []*LogEntryStruct{}
		return nil
	} else {

		entry := l.entries[index-1]
		if len(l.entries) > 0 && uint64(entry.TermIndex) != term {
			return errors.New(fmt.Sprintf("MRAFT.SLog: Entry at index does not have matching term (%v): (IDX=%v, TERM=%v)", entry.TermIndex, index, term))
		}

		if index < uint64(len(l.entries)) {
			buf := make([]byte, 8)

			for i := index; i < uint64(len(l.entries)); i++ {
				entry := l.entries[i]
				binary.LittleEndian.PutUint64(buf, uint64(entry.Logsn))
				err := l.db.Delete(buf, nil)
				if err != nil {
					panic("entry not exist")
				}
				if entry.Commit != nil {
					entry.Commit <- false
					close(entry.Commit)
					entry.Commit = nil
				}
			}
			l.entries = l.entries[0:index]
		}
	}
	return nil
}

func (l *Log) getCommitIndex() uint64 {
	l.RLock()
	defer l.RUnlock()
	return l.commitIndex
}

func (l *Log) getCommitIndexWithOutLock() uint64 {
	return l.commitIndex
}

func (l *Log) lastIndex() uint64 {
	l.RLock()
	defer l.RUnlock()
	return l.lastIndexWithOutLock()
}
func (l *Log) lastIndexWithOutLock() uint64 {
	if len(l.entries) <= 0 {
		return 0
	}
	return uint64(l.entries[len(l.entries)-1].Logsn)
}

func (l *Log) appendEntries(entries []*LogEntryStruct) error {
	l.Lock()
	defer l.Unlock()

	for i := range entries {
		if err := entries[i].writeToDB(l.db); err != nil {
			return err
		} else {
			l.entries = append(l.entries, entries[i])
		}
	}
	return nil
}

func (l *Log) appendEntry(entry *LogEntryStruct) error {
	l.Lock()
	defer l.Unlock()
	if len(l.entries) > 0 {
		lastTerm := l.lastTermWithOutLock()
		if uint64(entry.TermIndex) < lastTerm {
			return errTermIsSmall
		}
		lastIndex := l.lastIndexWithOutLock()
		if uint64(entry.TermIndex) == lastTerm && uint64(entry.Logsn) <= lastIndex {
			return errIndexIsSmall
		}
	}
	if err := entry.writeToDB(l.db); err != nil {
		return err
	}
	l.entries = append(l.entries, entry)

	if debug {
		fmt.Println("SERVER Appending LSN = ", entry.Logsn)
	}
	return nil
}

func (l *Log) updateCommitIndex(index uint64) {
	l.Lock()
	defer l.Unlock()
	if index > l.commitIndex {
		l.commitIndex = index
	}
}

func (l *Log) commitTill(commitIndex uint64) error {
	l.Lock()
	defer l.Unlock()
	if commitIndex > uint64(len(l.entries)) {
		commitIndex = uint64(len(l.entries))
	}
	if commitIndex < l.commitIndex {
		return nil
	}
	pos := l.commitIndex + 1
	if pos < 0 {
		panic("pending commit pos < 0")
	}
	for i := l.commitIndex + 1; i <= commitIndex; i++ {
		entryIndex := i - 1
		entry := l.entries[entryIndex]

		l.commitIndex = uint64(entry.Logsn)
		if entry.Commit != nil {

			entry.Commit <- true

			close(entry.Commit)
			entry.Commit = nil
		} else {

			l.ApplyFunc(entry)
		}
	}
	return nil
}

func (l *Log) commitInfo() (index uint64, term uint64) {
	l.RLock()
	defer l.RUnlock()
	if l.commitIndex == 0 {
		return 0, 0
	}
	if l.commitIndex == 0 {
		return 0, 0
	}
	entry := l.entries[l.commitIndex-1]
	return uint64(entry.Logsn), uint64(entry.TermIndex)
}

func (e *LogEntryStruct) writeToDB(db *leveldb.DB) error {
	var network bytes.Buffer
	enc := gob.NewEncoder(&network)
	err := enc.Encode(e)
	if err != nil {
		panic("gob error: " + err.Error())
	}
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(e.Logsn))
	err = db.Put(buf, []byte(network.String()), nil)
	return err
}

type SharedLog interface {
	ClusterComm()
	Append(data []byte, commandData Command) (LogEntry, error)
}

func (les LogEntryStruct) Term() uint64 {
	return les.TermIndex
}

func (les LogEntryStruct) Lsn() Lsn {
	return les.Logsn
}

func (les LogEntryStruct) Data() []byte {
	return les.DataArray
}

func (les LogEntryStruct) Committed() bool {
	return <-les.Commit
}

func (e ErrRedirect) Error() string {

	return "Redirect to server " + strconv.Itoa(int(e)) + "\r\n"
}

func AppendEntriesRPC(Servid int, ServerVar *mRaft) {

	env := <-(ServerVar.Inbox())
	env.Pid = env.SenderId
	env.MessageId = CONFIRMCONSENSUS
	env.SenderId = Servid
	ServerVar.Outbox() <- env

}

func (ServerVar *mRaft) Append(data []byte, commandData Command) (LogEntry, error) {

	var LogEnt LogEntry

	var err ErrRedirect
	if ServerVar.GetState() != LEADER {
		err = ErrRedirect(ServerVar.GetLeader())

		return LogEnt, err
	}

	var les LogEntryStruct

	(*ServerVar).LsnVar = (*ServerVar).LsnVar + 1
	les.Logsn = Lsn((*ServerVar).LsnVar)
	les.DataArray = data
	les.TermIndex = ServerVar.GetTerm()
	les.Commit = nil

	var msg appendEntries

	msg.TermIndex = ServerVar.Term

	msg.Entries = append(msg.Entries, &les)

	var envVar Envelope
	envVar.Pid = BROADCAST

	envVar.MessageId = APPENDENTRIES
	envVar.Leaderid = ServerVar.LeaderId
	envVar.SenderId = ServerVar.ServId()
	envVar.LastLogIndex = ServerVar.GetPrevLogIndex()
	envVar.LastLogTerm = ServerVar.GetPrevLogTerm()
	envVar.Message = msg
	MsgAckMap[les.Lsn()] = 1

	ServerVar.Outchan <- commandData

	ServerVar.Outbox() <- &envVar

	return les, nil
}
