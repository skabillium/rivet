package main

import (
	"fmt"
	"io"
	"log"
	"net"

	"github.com/c4pt0r/kvql"
)

type Engine struct {
}

func NewEngine() *Engine {
	return &Engine{}
}

func (ng *Engine) Execute(query string) (any, error) {
	parser := kvql.NewParser(query)
	stmt, err := parser.Parse()
	if err != nil {
		return nil, err
	}

	return "Executing statement: " + stmt.Name(), nil
}

type Config struct {
	Address string
}

type Server struct {
	Config
	*Engine
	Connections int
	ln          net.Listener
	quitCh      chan struct{}
}

func NewServer(config Config) *Server {
	return &Server{
		Config:      config,
		Engine:      NewEngine(),
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
		res, err := s.Engine.Execute(message)
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
