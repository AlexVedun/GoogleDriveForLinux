.PHONY: test vet build check qt-smoke appimage

LDFLAGS := $(if $(VERSION),-ldflags "-X google-drive-linux/internal/version.Version=$(VERSION)",)

test:
	go test ./...

vet:
	go vet ./...

build:
	mkdir -p bin
	go build $(LDFLAGS) -o bin/drivefs ./cmd/drivefs
	go build $(LDFLAGS) -o bin/driveapp ./cmd/driveapp

check: test vet build

qt-smoke: build
	QT_QPA_PLATFORM=offscreen ./bin/driveapp --smoke

appimage:
	./scripts/package-appimage.sh

