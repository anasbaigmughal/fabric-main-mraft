package mraft

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
)

var MutexAppend = &sync.RWMutex{}

type Keyvalstore interface {
	parseSetCas(cmdtype int, res []string, conn net.Conn)

	parseRest(cmdtype int, res []string, conn net.Conn)
	AppendUtility(conn net.Conn, command *bytes.Buffer, commandData *Command)

	ConnHandler(conn net.Conn)
}

func (ServerVar *mRaft) ClientServerComm(clientPortAddr string) {

	lis, error := net.Listen("tcp", clientPortAddr)
	defer lis.Close()

	if error != nil {
		panic("Client-Server connection error : " + error.Error())
	}

	go ServerVar.ApplyCommandToSM()
	go KvReadCommitCh()
	for {

		con, error := lis.Accept()
		if error != nil {
			panic("Client-Server accept error : " + error.Error())
			continue
		}

		go ServerVar.ConnHandler(con)

	}

}

func (ServerVar *mRaft) parseSetCas(cmdtype int, res []string, conn net.Conn, reader *bufio.Reader) {

	if cmdtype == 1 && ((len(res) != 4) || len(res[1]) > 250) {
		returnmsg := "ERRCMDERR\r\n"
		conn.Write([]byte(returnmsg))
	} else if cmdtype == 2 && ((len(res) != 5) || len(res[1]) > 250) {
		returnmsg := "ERRCMDERR\r\n"
		conn.Write([]byte(returnmsg))
	} else {
		exptime, err1 := strconv.Atoi(res[2])

		var numbytes int
		var version int64
		var err2, err3 error

		if cmdtype == 1 {

			version = -1
			err2 = nil
			numbytes, err3 = strconv.Atoi(res[3])

		} else {

			version, err2 = strconv.ParseInt(res[3], 10, 64)
			numbytes, err3 = strconv.Atoi(res[4])
		}

		if err1 != nil || err2 != nil || err3 != nil || exptime < 0 || numbytes < 0 {
			returnmsg := "ERRCMDERR\r\n"
			conn.Write([]byte(returnmsg))
			return
		}

		valuebyt, err := reader.ReadBytes('\n')
		if err != nil {
			returnmsg := "ERR_INTERNAL\r\n"
			conn.Write([]byte(returnmsg))
			return
		}

		valuebytes := string(valuebyt)

		valuebytes = strings.TrimSpace(valuebytes)

		if len(valuebytes) != numbytes {

			returnmsg := "ERRCMDERR\r\n"
			conn.Write([]byte(returnmsg))
			return
		}

		commandData := Command{cmdtype, res[1], exptime, numbytes, ([]byte(valuebytes)), version}

		command := new(bytes.Buffer)
		encCommand := gob.NewEncoder(command)
		encCommand.Encode(commandData)

		ServerVar.Inprocess = true
		ServerVar.AppendUtility(conn, command, commandData)

	}

}

func (ServerVar *mRaft) AppendUtility(conn net.Conn, command *bytes.Buffer, commandData Command) {

	if debug {
		fmt.Println("SERVERINFO: I am ", ServerVar.ServId(), "And PresentLeader is ", ServerVar.GetPresentLeader())
	}

	var err ErrRedirect
	if ServerVar.GetState() != PresentLeader {

		err = ErrRedirect(ServerVar.GetPresentLeader())

		returnmsg := err.Error()
		conn.Write([]byte(returnmsg))
		return

	}

	var les LogEntryStruct

	les.Logsn = Lsn(ServerVar.SLog.lastIndex() + 1)
	les.DataArray = command.Bytes()
	les.TermIndex = ServerVar.GetTerm()
	les.Commit = nil

	var msg appendEntries

	msg.TermIndex = ServerVar.Term

	msg.Entries = append(msg.Entries, &les)

	var envVar Envelope
	envVar.Pid = BROADCAST

	envVar.MessageId = APPENDENTRIES
	envVar.PresentLeaderid = ServerVar.PresentLeaderId
	envVar.SenderId = ServerVar.ServId()
	envVar.LastLogIndex = ServerVar.SLog.lastIndex()
	envVar.LastCandidateLogTerm = ServerVar.SLog.recentTerm()
	envVar.committedLength = ServerVar.SLog.commitIndex

	envVar.Message = msg
	MsgAckMap[les.Lsn()] = 1

	response := make(chan bool)
	temp := &CommandTuple{Com: command.Bytes(), ComResponse: response}

	ServerVar.Outchan <- temp

	var msgvar ConMsg
	msgvar.Les = les
	msgvar.Con = conn

	select {
	case t := <-response:
		if t {

			CommitCh <- msgvar

		} else {

			returnmsg := "ERR_INTERNAL\r\n"
			conn.Write([]byte(returnmsg))

		}
	}

}

func UpdateGlobLogEntMap(les LogEntry, conn net.Conn) {

	MutexLog.Lock()
	_, ok := LogEntMap[les.Lsn()]
	if !ok {
		LogEntMap[les.Lsn()] = conn
	}

	fmt.Println("UpdateGlobLogEntMap updated", les.Lsn(), "---", LogEntMap[les.Lsn()])
	MutexLog.Unlock()

}

func (ServerVar *mRaft) parseRest(cmdtype int, res []string, conn net.Conn, reader *bufio.Reader) {

	if len(res) != 2 {
		returnmsg := "ERRCMDERR\r\n"
		conn.Write([]byte(returnmsg))
	} else {
		commandData := Command{cmdtype, res[1], -1, -1, ([]byte("")), -1}
		command := new(bytes.Buffer)
		encCommand := gob.NewEncoder(command)
		encCommand.Encode(commandData)

		ServerVar.Inprocess = true
		ServerVar.AppendUtility(conn, command, commandData)

	}
}

func (ServerVar *mRaft) ConnHandler(conn net.Conn) {

	reader := bufio.NewReader(conn)

	for true {

		cmd, err := reader.ReadBytes('\n')

		var returnmsg string
		if err == io.EOF {
			returnmsg = "ERR_INTERNAL\r\n"
			io.Copy(conn, bytes.NewBufferString(returnmsg))
			break
		}

		if err != nil {
			returnmsg = "ERR_INTERNAL\r\n"
			io.Copy(conn, bytes.NewBufferString(returnmsg))
			break
		}
		command := string(cmd)
		command = strings.TrimSpace(command)

		res := strings.Split((command), " ")

		switch res[0] {
		case "set":
			ServerVar.parseSetCas(1, res, conn, reader)
			break
		case "cas":
			ServerVar.parseSetCas(2, res, conn, reader)
			break
		case "get":
			ServerVar.parseRest(3, res, conn, reader)
			break
		case "getm":
			ServerVar.parseRest(4, res, conn, reader)
			break
		case "delete":
			ServerVar.parseRest(5, res, conn, reader)
			break

		default:
			returnmsg = "ERRCMDERR\r\n"
			conn.Write([]byte(returnmsg))
			break
		}

	}

	conn.Close()

}
