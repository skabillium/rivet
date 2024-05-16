package main

import (
	"errors"
	"fmt"
	"strings"
)

type CommandType byte

const (
	CmdVersion CommandType = iota
	CmdKeys
	CmdAddFollower
)

type Command struct {
	Kind   CommandType
	Params map[string]string
}

func ParseCommand(command string) (*Command, error) {
	split := strings.Split(command, " ")

	params := map[string]string{}
	if len(split) > 1 {
		for i := 1; i < len(split); i++ {
			param := split[i]
			if strings.Contains(param, "=") {
				opts := strings.Split(param, "=")
				params[opts[0]] = opts[1]
			}
		}
	}

	cmd := strings.ToLower(split[0])
	switch cmd {
	case ".version":
		return &Command{Kind: CmdVersion}, nil
	case ".keys":
		return &Command{Kind: CmdKeys}, nil
	case ".add_follower":
		if params == nil || params["address"] == "" || params["node_id"] == "" {
			return nil, errors.New("params 'address' and 'node_id' are required for add_follower command")
		}
		return &Command{Kind: CmdAddFollower, Params: params}, nil
	default:
		return nil, fmt.Errorf("command '%s' not supported", cmd)
	}
}
