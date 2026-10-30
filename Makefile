.PHONY: test vet build check qt-smoke appimage

test:
	go test ./...

vet:
	go vet ./...

build:
	mkdir -p bin
	go build -o bin/drivefs ./cmd/drivefs
	go build -o bin/driveapp ./cmd/driveapp

check: test vet build

qt-smoke: build
	QT_QPA_PLATFORM=offscreen ./bin/driveapp --smoke

appimage:
	./scripts/package-appimage.sh

