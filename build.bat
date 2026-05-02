@echo off

set CGO_ENABLED=0
set GOARCH=amd64

if not exist bin mkdir bin

go mod download
if %errorlevel% neq 0 goto fail

echo Building for WINDOWS...
set GOOS=windows
go build -buildvcs=false -ldflags="-w -s" -o bin/tracker.exe ./cmd/tracker
if %errorlevel% neq 0 goto fail
go build -buildvcs=false -ldflags="-w -s" -o bin/p2pcdn.exe ./cmd/cli
if %errorlevel% neq 0 goto fail
go build -buildvcs=false -ldflags="-w -s" -o bin/p2pcdn-daemon.exe ./cmd/daemon
if %errorlevel% neq 0 goto fail
go build -buildvcs=false -ldflags="-w -s" -o bin/benchmark.exe ./cmd/benchmark
if %errorlevel% neq 0 goto fail

echo Building for LINUX...
set GOOS=linux
go build -buildvcs=false -ldflags="-w -s" -o bin/tracker ./cmd/tracker
if %errorlevel% neq 0 goto fail
go build -buildvcs=false -ldflags="-w -s" -o bin/p2pcdn ./cmd/cli
if %errorlevel% neq 0 goto fail
go build -buildvcs=false -ldflags="-w -s" -o bin/p2pcdn-daemon ./cmd/daemon
if %errorlevel% neq 0 goto fail
go build -buildvcs=false -ldflags="-w -s" -o bin/benchmark ./cmd/benchmark
if %errorlevel% neq 0 goto fail

echo.
echo All builds successful
exit /b 0

:fail
echo.
echo Build failed!
exit /b 1