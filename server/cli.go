package main

import (
	"flag"
)

// Storage options
const (
	StgDisk = iota
	StgMem
)

type CLIOptions struct {
	serverPort  string
	raftPort    string
	raftNodeId  string
	storage     int
	user        string
	password    string
	authEnabled bool
}

func ParseCLIOptions() CLIOptions {
	var (
		serverPort string
		raftPort   string
		raftNodeId string
		storageStr string
		user       string
		password   string
		noAuth     bool
	)

	flag.StringVar(&serverPort, "port", DefaultRivetPort, "Port to run Rivet server")
	flag.StringVar(&raftPort, "raft-port", DefaultRaftPort, "Port to run RAFT server")
	flag.StringVar(&raftNodeId, "node-id", "", "RAFT node id")
	flag.StringVar(&storageStr, "storage", DefaultStorage, "Where to store the data, 'memory' or 'disk'")
	flag.StringVar(&user, "user", DefaultUser, "User")
	flag.StringVar(&password, "password", DefaultPassword, "Password")
	flag.BoolVar(&noAuth, "noauth", false, "Disable authentication requirement")
	flag.Parse()

	assert(serverPort != raftPort, "Rivet and RAFT port need to be different")
	assert(raftNodeId != "", "RAFT node id is required")
	assert(storageStr == "memory" || storageStr == "disk", "Only 'disk' and 'memory' are valid storage options")

	storage := StgDisk
	if storageStr == "memory" {
		storage = StgMem
	}

	return CLIOptions{
		serverPort:  serverPort,
		raftPort:    raftPort,
		raftNodeId:  raftNodeId,
		storage:     storage,
		user:        user,
		password:    password,
		authEnabled: !noAuth,
	}
}
