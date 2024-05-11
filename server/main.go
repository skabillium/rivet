package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"strings"

	"github.com/c4pt0r/kvql"
	"go.etcd.io/bbolt"
)

type KV struct {
	db     *bbolt.DB
	bucket []byte
}

type KVCursor struct{}

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

func (kv *KV) BatchPut(kvs []kvql.KVPair) error { return nil }
func (kv *KV) BatchDelete(keys [][]byte) error  { return nil }
func (kv *KV) Cursor() (kvql.Cursor, error)     { return nil, nil }

func (kv *KV) Close() {
	kv.db.Close()
}

func Execute(storage kvql.Storage, query string) (any, error) {
	query = strings.TrimSpace(query)
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
	for _, row := range rows {
		for _, col := range row {
			switch col := col.(type) {
			case int, int32, int64:
				fmt.Printf("%d ", col)
			case []byte:
				fmt.Printf("%s ", string(col))
			default:
				fmt.Printf("%v ", col)
			}
		}
		fmt.Println()
	}

	return rows, nil
}

type Config struct {
	Address string
}

type Server struct {
	Config
	*KV
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

	// Init db
	db, err := bbolt.Open("rivet-data/default.db", 0600, nil)
	if err != nil {
		return err
	}
	s.KV, err = KVInit(db)
	if err != nil {
		return err
	}
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

		message := string(buf[:n])
		res, err := Execute(s.KV, message)
		if err != nil {
			conn.Write([]byte("[ERROR]: " + err.Error() + "\n"))
		} else {
			if res != nil {
				conn.Write([]byte(fmt.Sprintln(res)))
			} else {
				conn.Write([]byte("OK \n"))
			}
		}
	}
}

func main() {
	server := NewServer(Config{Address: "localhost:5678"})
	if err := server.Start(); err != nil {
		log.Fatal(err)
	}
}
