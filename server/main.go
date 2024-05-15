package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"path"
	"skabillium/rivet/storage"
	"strings"
	"time"

	"github.com/c4pt0r/kvql"
	"github.com/hashicorp/raft"
)

func ExecuteCommand(kv storage.Storage, command string) (any, error) {
	switch command {
	case ".version":
		return RivetVersion, nil
	case ".keys":
		return kv.Keys(), nil
	}
	return nil, fmt.Errorf("unknown command '%s'", command)
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

func Execute(kv storage.Storage, message string) (any, error) {
	if message[0] == '.' {
		return ExecuteCommand(kv, message)
	}
	return ExecuteQuery(kv, message)
}

type Config struct {
	Address string
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
		go s.handleConnection(conn)
	}
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

		if message[0] == '.' {
			res, err := ExecuteCommand(s.KV, message)
			if err != nil {
				conn.Write([]byte(fmt.Sprintln("[ERROR]:", err)))
			} else {
				conn.Write([]byte(fmt.Sprintln(res)))
			}
			continue
		}

		parser := kvql.NewParser(message)
		stmt, err := parser.Parse()
		if err != nil {
			WriteError(conn, err)
			continue
		}

		if stmt.Name() == "SELECT" {
			rows, err := ExecuteQuery(s.KV, message)
			if err != nil {
				conn.Write([]byte("[ERROR]: " + err.Error() + "\n"))
				continue
			}

			if rows == nil {
				conn.Write([]byte("OK \n"))
				continue
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

			conn.Write([]byte(fmt.Sprintln(results)))
		}

		future := s.raft.Apply([]byte(message), 500*time.Millisecond)
		if err := future.Error(); err != nil {
			WriteError(conn, fmt.Errorf("could not apply: %s", err))
		}

		e := future.Response()
		if e != nil {
			WriteError(conn, fmt.Errorf("could not apply (internal): %s", e))
		}

	}
}

func WriteError(conn net.Conn, err error) {
	conn.Write([]byte(fmt.Sprintln("[ERROR:", err)))
}

func assert(cond bool, message any) {
	if !cond {
		panic(fmt.Sprint("Assertion failed: ", message))
	}
}

func main() {
	cliOpts := ParseCLIOptions()

	var err error
	server := NewServer(Config{Address: "localhost:" + cliOpts.serverPort})
	server.KV, err = storage.StorageInit(storage.InitStorageOptions{
		Disk: &storage.DiskStorageOptions{
			File: "rivet-data/default.db",
		},
	})
	assert(err == nil, err)

	fsm := &RivetFsm{kv: server.KV}
	r, err := RaftInit(path.Join(DataDir, cliOpts.raftNodeId), "raft_1", "localhost:"+cliOpts.raftPort, fsm)
	if err != nil {
		log.Fatal(err)
	}

	server.raft = r

	if err := server.Start(); err != nil {
		log.Fatal(err)
	}
}
