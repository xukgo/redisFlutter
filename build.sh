#!/usr/bin/env bash

CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags "$LDFlags" -o out/syncReader server/syncReader/*.go
upx out/syncReader

#CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags "$LDFlags" -o out/syncWriter server/syncWriter/*.go
#upx out/syncWriter
