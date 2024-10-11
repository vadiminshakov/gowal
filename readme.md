![tests](https://github.com/vadiminshakov/gowal/actions/workflows/tests.yml/badge.svg?branch=main)
[![Go Reference](https://pkg.go.dev/badge/github.com/vadiminshakov/gowal.svg)](https://pkg.go.dev/github.com/vadiminshakov/gowal)
[![Go Report Card](https://goreportcard.com/badge/github.com/vadiminshakov/gowal)](https://goreportcard.com/report/github.com/vadiminshakov/gowal)

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

### Retrieving a log entry

You can retrieve a log entry by its index:

```go
key, value, found := wal.Get(1)
if !found {
    log.Println("Entry not found")
} else {
    log.Printf("Key: %s, Value: %s", key, string(value))
}
```

### Iterating over log entries

You can iterate over all log entries using the `Iterate` function:

```go
iter := wal.Iterator()
for msg, ok := iter() ; ok; msg, ok = iter() {
    log.Printf("Key: %s, Value: %s\n", msg.Key, string(msg.Value))
}
```

### Closing the WAL
Always ensure that you close the WAL instance to properly flush and close the log files:

```go
err := wal.Close()
if err != nil {
    log.Fatal(err)
}
```

### Configuration
The behavior of the WAL can be configured using several configuration options (`Config` parameter in the `NewWAL` function):

 - `SegmentThreshold`: Maximum number of log entries per segment before rotation occurs. Default is 1000.
 - `MaxSegments`: Maximum number of segments to keep before the oldest segments are deleted. Default is 5.
 - `IsInSyncDiskMode`: When set to true, every write is synced to disk, ensuring durability at the cost of performance. Default is false.

### Contributing
Feel free to open issues or submit pull requests for improvements and bug fixes. We welcome contributions!

### License
This project is licensed under the Apache License.