.PHONY: test vet build check

test:
	go test ./...

vet:
	go vet ./...

build:
	mkdir -p bin
	go build -o bin/drivefs ./cmd/drivefs
	go build -o bin/driveapp ./cmd/driveapp

check: test vet build

