TEST = ./test
BIN = ./bin/rivet

install:
	go mod download

clean:
	rm -rf ./rivet-data
	rm -rf ./bin

tests:
	go test ./server

dev:
	go run ./server --node-id=node_1

build:
	go build -o ${BIN} ./server

start: clean build
	${BIN}
