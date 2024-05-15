package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path"
	"skabillium/rivet/storage"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

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

type RivetFsm struct {
	kv storage.Storage
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

// No-operation implementation of FSMSnapshot
type SnapshotNoop struct{}

func (s *SnapshotNoop) Persist(sink raft.SnapshotSink) error {
	return sink.Cancel()
}

func (s *SnapshotNoop) Release() {}
