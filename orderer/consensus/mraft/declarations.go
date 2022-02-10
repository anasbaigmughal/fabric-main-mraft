package mraft

import (
	"errors"
	"net"
	"sync"
	"time"

	zmq "github.com/pebbe/zmq4"
	"github.com/syndtr/goleveldb/leveldb"
)

type ErrRedirect int

var MsgAckMap map[Lsn]int

var LogEntMap map[Lsn]net.Conn

var CommitCh chan ConMsg

var keyval = make(map[string]valstruct)

var mutex = &sync.RWMutex{}
var MutexLog = &sync.RWMutex{}

type PriorityQueue []*Item

const (
	BROADCAST = -1

	NOVOTE         = -1
	UNKNOWN        = -1
	INITIALIZATION = 0

	APPENDENTRIESRPC         = 1
	CONFIRMCONSENSUS         = 2
	REQUESTVOTE              = 3
	VOTERESPONSE             = 4
	APPENDENTRIES            = 5
	APPENDENTRIESRESPONSE    = 6
	HEARTBEATRESPONSE        = 7
	HEARTBEAT                = 8
	APPENDENTRIESRPCRESPONSE = 9

	FOLLOWER  = 1
	CANDIDATE = 2
	LEADER    = 3
)

var (
	errNoCommand = errors.New("NO COMMAND")
	errorTimeout = errors.New("TIMEOUT")

	errWrongIndex              = errors.New("BAD INDEX")
	errWrongTerm               = errors.New("BAD TERM")
	errTermIsSmall             = errors.New("TOO_SMALL_TERM")
	errorappendEntriesRejected = errors.New("APPENDENTRIES_REJECTED")
	errIndexIsSmall            = errors.New("TOO_SMALL_INDEX")
	errIndexIsBig              = errors.New("TOO_BIG_COMMIT_INDEX")
	errorDeposed               = errors.New("DEPOSED")
	errorOutOfSync             = errors.New("OUTOFSYNC")

	errChecksumInvalid = errors.New("INVALID CHECKSUM")

	voteLock = &sync.RWMutex{}

	voteMap = make(map[int]bool)

	MinElectTo int32 = 100
	MaxElectTo       = 3 * MinElectTo
)

type Log struct {
	sync.RWMutex
	ApplyFunc   func(*LogEntryStruct)
	db          *leveldb.DB
	entries     []*LogEntryStruct
	commitIndex uint64
	initialTerm uint64
}

var debug = false

type Lsn uint64

var LeaderID, Quorum int

type uint64Slice []uint64

func (p uint64Slice) Len() int           { return len(p) }
func (p uint64Slice) Less(i, j int) bool { return p[i] > p[j] }
func (p uint64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

type Envelope struct {
	Pid          int
	SenderId     int
	Leaderid     int
	MessageId    int
	CommitIndex  uint64
	LastLogIndex uint64
	LastLogTerm  uint64

	Message interface{}
}

type LogEntry interface {
	Lsn() Lsn
	Data() []byte
	Term() uint64
	Committed() bool
}

type LogEntryStruct struct {
	Logsn     Lsn
	TermIndex uint64
	DataArray []byte
	Commit    chan bool
}

type CommandTuple struct {
	Com         []byte
	ComResponse chan bool
	Err         chan error
}

type valstruct struct {
	version    int64
	expirytime int
	timestamp  int64
	numbytes   int
	value      []byte
}

type Command struct {
	CmdType    int
	Key        string
	Expirytime int
	Len        int
	Value      []byte
	Version    int64
}

type ConMsg struct {
	Les LogEntryStruct
	Con net.Conn
}

var PQ = make(PriorityQueue, 0)

type Item struct {
	value     string
	priority  int64
	timestamp int64
	index     int
}

type ServerConfig struct {
	Id         string
	HostName   string
	ClientPort string
	LogPort    string
}

type clusterCount struct {
	Count string
}

type serverLogPath struct {
	Path string
}

type ClusterConfig struct {
	Path    serverLogPath
	Servers []ServerConfig
	Count   clusterCount
}

type nextIndex struct {
	sync.RWMutex
	m map[uint64]uint64
}

type appendEntries struct {
	TermIndex uint64
	Entries   []*LogEntryStruct
}

type appendEntriesResponse struct {
	Term    uint64
	Success bool
	reason  string
}

type mRaft struct {
	Pid           int
	Peers         []int
	Path          string
	Term          uint64
	VotedFor      int
	VotedTerm     int
	LeaderId      int
	CommitIndex   uint64
	MatchIndex    map[int]uint64
	NextIndex     map[int]uint64
	PrevLogIndex  uint64
	PrevLogTerm   uint64
	ElectTicker   <-chan time.Time
	State         int
	LastApplied   uint64
	In            chan *Envelope
	Out           chan *Envelope
	Address       map[int]string
	ClientSockets map[int]*zmq.Socket
	LogSockets    map[int]*zmq.Socket
	LsnVar        uint64
	ClusterSize   int
	GotConsensus  chan bool
	SLog          *Log
	Inchan        chan *LogEntryStruct
	Outchan       chan interface{}
	Inprocess     bool
}
