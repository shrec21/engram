.PHONY: proto build test run clean

proto:
	@mkdir -p proto/engrampb
	protoc \
		--go_out=proto/engrampb --go_opt=paths=source_relative \
		--go-grpc_out=proto/engrampb --go-grpc_opt=paths=source_relative \
		-I proto \
		proto/engram.proto

build:
	go build ./cmd/engramd/
	go build ./cmd/engramctl/

test:
	go test -race ./...

run: build
	./engramd

clean:
	rm -f engramd engramctl
	rm -rf engram-data
