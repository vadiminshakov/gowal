# GoWAL - Write-Ahead Logging in Go

GoWAL is a simple, efficient **Write-Ahead Log (WAL)** library written in Go. It allows you to store logs as an append-only log structure, which is useful for applications that require crash recovery, transaction logging, or high-availability systems. GoWAL is optimized for performance with configurable segment rotation and in-memory indexing.

## Features

- **Append-only log structure**: Ensures that data is only appended and never modified or deleted, which is crucial for durability and integrity.
- **Segmented logs**: Automatically rotates segments after a configurable threshold is reached. Older segments are deleted to save disk space.
- **Efficient lookups**: In-memory index allows for quick lookups of log entries by their index.
- **Persistence**: Logs and their indexes are stored on disk and reloaded into memory upon initialization.
- **Configurable sync mode**: Option to sync logs to disk after every write to ensure data durability, though at the cost of speed.

## Installation

```bash
go get github.com/vadiminshakov/gowal
```

## Usage

### Initialization

To create a new WAL instance, specify the directory to store logs and a prefix for the log files:

```go
import "github.com/vadiminshakov/gowal"

wal, err := gowal.NewWAL("/path/to/logs", "prefix")
if err != nil {
    log.Fatal(err)
}
defer wal.Close()
```

### Adding a log entry
You can append a new log entry by providing an index, a key, and a value:
```go
err := wal.Set(1, "myKey", []byte("myValue"))
if err != nil {
    log.Fatal(err)
}
```
If the entry with the same index already exists, the function will return an error.

