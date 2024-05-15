package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path"
	"strings"
	"time"

	"github.com/c4pt0r/kvql"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"go.etcd.io/bbolt"
)

const RivetVersion = "0.0.1"
const DataDir = "rivet-data"

type KV struct {
	db     *bbolt.DB
	bucket []byte
}

type KVCursor struct {
	prefix  []byte
	currKey []byte
	currVal []byte
	bucket  []byte
	db      *bbolt.DB
}

func (c *KVCursor) Seek(prefix []byte) error {
	c.prefix = prefix
	return c.db.View(func(tx *bbolt.Tx) error {
		cursor := tx.Bucket(c.bucket).Cursor()
		k, v := cursor.Seek(prefix)
		c.currKey = k
		c.currVal = v
		return nil
	})
}

func (c *KVCursor) Next() (key []byte, value []byte, err error) {
	var prevKey, prevVal []byte
	c.db.View(func(tx *bbolt.Tx) error {
		prevKey = c.currKey
		prevVal = c.currVal
		cursor := tx.Bucket(c.bucket).Cursor()
		cursor.Seek(prevKey)
		k, v := cursor.Next()
		if bytes.HasPrefix(k, c.prefix) {
			c.currKey, c.currVal = k, v
		} else {
			c.prefix, c.currKey, c.currVal = nil, nil, nil
		}
		return nil
	})
	return prevKey, prevVal, nil
}

func KVInit(db *bbolt.DB) (*KV, error) {
	kv := &KV{db: db, bucket: []byte("data")}
	err := db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(kv.bucket)
		return err
	})
	if err != nil {
		return nil, err
	}
	return kv, nil
}

func (kv *KV) Keys() []string {
	var keys []string
	kv.db.View(func(tx *bbolt.Tx) error {
		buck := tx.Bucket(kv.bucket)
		buck.ForEach(func(k, v []byte) error {
			keys = append(keys, string(k))
			return nil
		})
		return nil
	})
	return keys
}

func (kv *KV) Get(key []byte) ([]byte, error) {
	var value []byte
	kv.db.View(func(tx *bbolt.Tx) error {
		buck := tx.Bucket(kv.bucket)
		value = buck.Get(key)
		return nil
	})
	return value, nil
}

func (kv *KV) Put(key []byte, value []byte) error {
	return kv.db.Update(func(tx *bbolt.Tx) error {
		buck := tx.Bucket(kv.bucket)
		return buck.Put(key, value)
	})
}

func (kv *KV) Delete(key []byte) error {
	return kv.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(kv.bucket).Delete(key)
	})
}

func (kv *KV) BatchPut(kvs []kvql.KVPair) error {
	return kv.db.Update(func(tx *bbolt.Tx) error {
		buck := tx.Bucket(kv.bucket)
		var err error
		for _, pair := range kvs {
			err = buck.Put(pair.Key, pair.Value)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (kv *KV) BatchDelete(keys [][]byte) error {
	return kv.db.Update(func(tx *bbolt.Tx) error {
		buck := tx.Bucket(kv.bucket)
		var err error
		for _, k := range keys {
			err = buck.Delete(k)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (kv *KV) Cursor() (kvql.Cursor, error) {
	return &KVCursor{
		bucket: kv.bucket,
		db:     kv.db,
	}, nil
}

func (kv *KV) Close() {
	kv.db.Close()
}

func ExecuteCommand(kv *KV, command string) (any, error) {
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

func Execute(kv *KV, message string) (any, error) {
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
	*KV
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
			WriteError(conn, fmt.Errorf("Could not apply (internal): %s", e))
		}

	}
}

func WriteError(conn net.Conn, err error) {
	conn.Write([]byte(fmt.Sprintln("[ERROR:", err)))
}

func assert(cond bool, message any) {
	if !cond {
		panic(fmt.Sprint("Assertion failed:", message))
	}
}

type SnapshotNoop struct{}

func (s *SnapshotNoop) Persist(sink raft.SnapshotSink) error {
	return sink.Cancel()
}

func (s *SnapshotNoop) Release() {}

type RivetFsm struct {
	kv *KV
}

func (rf *RivetFsm) Apply(log *raft.Log) any {
	switch log.Type {
	case raft.LogCommand:
		message := string(log.Data)
		_, err := Execute(rf.kv, message)
		assert(err == nil, err)
	default:
		panic(fmt.Errorf("unknown log type %#v", log.Type))
	}
	return nil
}

func (rf *RivetFsm) Restore(rc io.ReadCloser) error {
	return errors.New("nothing to restore")
}

func (rf *RivetFsm) Snapshot() (raft.FSMSnapshot, error) {
	return &SnapshotNoop{}, nil
}

func RaftInit(dir, nodeId, address string, rf *RivetFsm) (*raft.Raft, error) {
	os.MkdirAll(dir, os.ModePerm)
	store, err := raftboltdb.NewBoltStore(path.Join(dir, "raft"))
	if err != nil {
		return nil, err
	}

	snapshots, err := raft.NewFileSnapshotStore(path.Join(dir, "snapshot"), 2, os.Stderr)
	if err != nil {
		return nil, err
	}

	tcpAddress, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		return nil, err
	}

	transport, err := raft.NewTCPTransport(address, tcpAddress, 10, time.Second*10, os.Stderr)
	if err != nil {
		return nil, err
	}

	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(nodeId)

	r, err := raft.NewRaft(config, rf, store, store, snapshots, transport)
	if err != nil {
		return nil, err
	}

	// All cluster instances start off as leaders, picking is done
	// manually after startup, maybe we can change this later
	r.BootstrapCluster(raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      raft.ServerID(nodeId),
				Address: transport.LocalAddr(),
			},
		},
	})

	return r, nil
}

func main() {
	err := os.MkdirAll("rivet-data", os.ModePerm)
	if err != nil {
		log.Fatal(err)
	}

	db, err := bbolt.Open("rivet-data/default.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	server := NewServer(Config{Address: "localhost:5678"})

	server.KV, err = KVInit(db)
	if err != nil {
		log.Fatal(err)
	}

	fsm := &RivetFsm{kv: server.KV}

	nodeId := "raft_1"
	raftPort := "8765"

	r, err := RaftInit(path.Join(DataDir, nodeId), "raft_1", "localhost:"+raftPort, fsm)
	if err != nil {
		log.Fatal(err)
	}

	server.raft = r

	if err := server.Start(); err != nil {
		log.Fatal(err)
	}
}
