package main

import (
	"bytes"
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

func Execute(storage kvql.Storage, query string) ([][]kvql.Column, error) {
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
		rows, err := Execute(s.KV, message)
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
}

func main() {
	server := NewServer(Config{Address: "localhost:5678"})
	if err := server.Start(); err != nil {
		log.Fatal(err)
	}
}
