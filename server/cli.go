package main

import "flag"

type CLIOptions struct {
	serverPort string
	raftPort   string
	raftNodeId string
}

func ParseCLIOptions() CLIOptions {
	var (
		serverPort string
		raftPort   string
		raftNodeId string
	)

	flag.StringVar(&serverPort, "port", DefaultRivetPort, "Port to run Rivet server")
	flag.StringVar(&raftPort, "raft-port", DefaultRaftPort, "Port to run RAFT server")
	flag.StringVar(&raftNodeId, "node-id", "", "RAFT node id")
	flag.Parse()

	assert(serverPort != raftPort, "Rivet and RAFT port need to be different")
	assert(raftNodeId != "", "RAFT node id is required")

	return CLIOptions{
		serverPort: serverPort,
		raftPort:   raftPort,
		raftNodeId: raftNodeId,
	}
}
