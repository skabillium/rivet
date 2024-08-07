package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path"
	"skabillium/rivet/storage"
	"strings"
	"time"
	"unicode"

	"github.com/c4pt0r/kvql"
	"github.com/hashicorp/raft"
)

func ExecuteCommand(kv storage.Storage, r *raft.Raft, command string) (any, error) {
	cmd, err := ParseCommand(command)
	if err != nil {
		return nil, err
	}
	switch cmd.Kind {
	case CmdVersion:
		return RivetVersion, nil
	case CmdKeys:
		return kv.Keys(), nil
	case CmdAddFollower:
		if r.State() != raft.Leader {
			return nil, errors.New("cannot add follower to a non-leader node")
		}
		err := r.AddVoter(raft.ServerID(cmd.Params["node_id"]), raft.ServerAddress(cmd.Params["address"]), 0, 0).Error()
		if err != nil {
			return nil, err
		}
		return "OK", nil
	}
	return nil, nil
}

func ExecuteQuery(storage kvql.Storage, query string) ([][]kvql.Column, error) {
	opt := kvql.NewOptimizer(query)

	plan, err := opt.BuildPlan(storage)
	if err != nil {
		return nil, err
	}

	ctx := kvql.NewExecuteCtx()

	rows, err := plan.Batch(ctx)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	ctx.Clear()
	return rows, nil
}

type Config struct {
	Address     string
	User        string
	Password    string
	AuthEnabled bool
}

type Server struct {
	Config
	KV          storage.Storage
	raft        *raft.Raft
	Connections int
	ln          net.Listener
	quitCh      chan struct{}
}

func NewServer(config Config) *Server {
	return &Server{
		Config:      config,
		Connections: 0,
		quitCh:      make(chan struct{}),
	}
}

func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.Address)
	if err != nil {
		return err
	}
	s.ln = ln
	defer s.KV.Close()

	go s.acceptLoop()

	log.Println("Server started at", s.Address)

	<-s.quitCh

	return nil
}
func (s *Server) acceptLoop() error {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			log.Println("[ERROR]:", err)
			continue
		}

		if s.Config.AuthEnabled {
			err = s.handleHandshake(conn)
			if err != nil {
				WriteError(conn, err)
				conn.Close()
				continue
			}
		}
		go s.handleConnection(conn)
	}
}

func (s *Server) handleHandshake(conn net.Conn) error {
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		return err
	}
	message := strings.TrimSpace(string(buf[:n]))
	cmd, err := ParseCommand(message)
	if err != nil {
		return err
	}
	if cmd.Kind != CmdAuth {
		return errors.New("only 'auth' command allowed for handshake")
	}
	if cmd.Params["user"] != s.Config.User || cmd.Params["password"] != s.Config.Password {
		return errors.New("invalid auth credentials")
	}
	return nil
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	for {
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			if err != io.EOF {
				log.Println("[ERROR]:", err)
			}
			break
		}
		message := strings.TrimSpace(string(buf[:n]))
		if len(message) == 0 {
			continue
		}
		s.handleMessage(conn, message)
	}
}

func (s *Server) handleMessage(conn net.Conn, message string) {
	if message[0] == '.' {
		res, err := ExecuteCommand(s.KV, s.raft, message)
		if err != nil {
			WriteError(conn, err)
		} else {
			serialized, err := json.Marshal(res)
			if err != nil {
				WriteError(conn, errors.New("could not serialize response"))
				return
			}
			Writeln(conn, serialized)
		}
		return
	}

	if isSelectStatement(message) {
		rows, err := ExecuteQuery(s.KV, message)
		if err != nil {
			WriteError(conn, err)
			return
		}

		if rows == nil {
			WriteOk(conn)
			return
		}

		results := make([]string, len(rows))
		for _, row := range rows {
			var res string
			for _, col := range row {
				switch col := col.(type) {
				case int, int32, int64:
					res += fmt.Sprintf("%d ", col)
				case []byte:
					res += fmt.Sprintf("%s ", string(col))
				default:
					res += fmt.Sprintf("%v ", col)
				}
			}
			results = append(results, res)
		}

		serialized, err := json.Marshal(results)
		if err != nil {
			WriteError(conn, errors.New("could not format response"))
			return
		}

		Writeln(conn, serialized)
		return
	}

	future := s.raft.Apply([]byte(message), 500*time.Millisecond)
	if err := future.Error(); err != nil {
		WriteError(conn, fmt.Errorf("could not apply: %s", err))
		return
	}

	res := future.Response()
	serialized, err := json.Marshal(res)
	if err != nil {
		WriteError(conn, err)
		return
	}
	Writeln(conn, serialized)
}

func WriteOk(conn net.Conn) {
	conn.Write([]byte("OK\n"))
}

func WriteError(conn net.Conn, err error) {
	conn.Write([]byte(fmt.Sprintln("[ERROR]:", err)))
}

func Writeln(conn net.Conn, b []byte) {
	b = append(b, '\n')
	conn.Write(b)
}

func assert(cond bool, message ...any) {
	if !cond {
		if len(message) > 0 {
			fmt.Println("Assertion failed:", message)
			os.Exit(1)
		}
	}
}

func isSelectStatement(s string) bool {
	for i, c := range s {
		if unicode.IsSpace(c) {
			continue
		}
		return strings.ToLower(s[i:i+6]) == "select"
	}
	return false
}

func main() {
	cliOpts := ParseCLIOptions()
	nodeDir := path.Join(DataDir, cliOpts.raftNodeId)

	var err error
	server := NewServer(Config{
		Address:     "localhost:" + cliOpts.serverPort,
		User:        cliOpts.user,
		Password:    cliOpts.password,
		AuthEnabled: cliOpts.authEnabled,
	})

	initStorageOpts := storage.InitStorageOptions{}
	if cliOpts.storage == StgDisk {
		initStorageOpts.Disk = &storage.DiskStorageOptions{
			File: path.Join(nodeDir, "default.db"),
		}
	}

	server.KV, err = storage.StorageInit(initStorageOpts)
	assert(err == nil, err)

	fsm := &RivetFsm{kv: server.KV}
	server.raft, err = RaftInit(nodeDir, cliOpts.raftNodeId, "localhost:"+cliOpts.raftPort, fsm)
	if err != nil {
		log.Fatal(err)
	}

	if err := server.Start(); err != nil {
		log.Fatal(err)
	}
}
