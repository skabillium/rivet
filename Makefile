CMD = ./cmd
TEST = ./test
BIN = ./bin/rivet

install:
	go mod download

clean:
	rm -rf ./bin

tests:
	go test ${CMD}/db
	go test ${CMD}/resp
	go test ${CMD}

dev:
	go run ${CMD}

build:
	go build -o ${BIN} ${CMD}

start: clean build
	${BIN}
