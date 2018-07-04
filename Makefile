all: build

build: 
	go build -o bin/read read/read.go
	go build -o bin/write write/write.go

