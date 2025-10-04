![tests](https://github.com/vadiminshakov/gowal/actions/workflows/tests.yml/badge.svg?branch=main)
[![Go Reference](https://pkg.go.dev/badge/github.com/vadiminshakov/gowal.svg)](https://pkg.go.dev/github.com/vadiminshakov/gowal)
[![Go Report Card](https://goreportcard.com/badge/github.com/vadiminshakov/gowal)](https://goreportcard.com/report/github.com/vadiminshakov/gowal)

# GoWAL - Write-Ahead Logging in Go

GoWAL is a simple, efficient **Write-Ahead Log (WAL)** library written in Go.
It allows you to store data in an append-only log structure, which is useful for applications that require crash recovery, transaction logging, or high-availability systems. 
GoWAL is optimized for performance with configurable segment rotation and in-memory indexing.


## Installation

```bash
go get github.com/vadiminshakov/gowal
```

## Usage

### Initialization

To create a new WAL instance, specify the directory to store logs and a prefix for the log files:

```go
import "github.com/vadiminshakov/gowal"

cfg := gowal.Config{
    Dir:    "./log",
    Prefix: "segment_",
    SegmentThreshold: 1000,
    MaxSegments:      100,
    IsInSyncDiskMode: false,
}

wal, err := gowal.NewWAL(cfg)
if err != nil {
    log.Fatal(err)
}
defer wal.Close()
```

### Adding a log entry
You can append a new log entry by providing an index, a key, and a value:
```go
err := wal.Write(1, "myKey", []byte("myValue"))
if err != nil {
    log.Fatal(err)
}
```
If the entry with the same index already exists, the function will return an error.

### Retrieving a log entry

You can retrieve a log entry by its index:

```go
key, value, err := wal.Get(1)
if err != nil {
    log.Println("Entry not found or error:", err)
} else {
    log.Printf("Key: %s, Value: %s", key, string(value))
}
```

### Iterating over log entries

You can iterate over all log entries using the `Iterator` function:

```go
for msg := range wal.Iterator() {
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

### Recover corrupted WAL
If the WAL is corrupted, you can recover it by calling the `UnsafeRecover` function:

```go
removedFiles, err := gowal.UnsafeRecover("./log", "segment_")
if err != nil {
    log.Fatal(err)
}
log.Printf("Removed corrupted files: %v", removedFiles)
```

### Configuration
The behavior of the WAL can be configured using several configuration options (`Config` parameter in the `NewWAL` function):

 - `SegmentThreshold`: Maximum number of log entries per segment before rotation occurs. Default is 1000.
 - `MaxSegments`: Maximum number of segments to keep before the oldest segments are deleted. Default is 5.
 - `IsInSyncDiskMode`: When set to true, every write is synced to disk, ensuring durability at the cost of performance. Default is false.

## Architecture

GoWAL uses a segmented architecture with two-level indexing for efficient write and read operations:

#### Segments
Data is split into numbered files (`segment_0`, `segment_1`, etc.). Each record contains:
- Index, Key, Value
- CRC32 checksum for integrity verification

#### Two-Level Indexing
- **tmpIndex**: In-memory index for the current active segment. Maps record index to its position in the segment.
- **index**: Main in-memory index for all persisted (closed) segments. Provides fast lookups across historical data.

#### Write Flow
1. Check if index already exists (prevents duplicates)
2. Check if rotation is needed based on `SegmentThreshold`
3. Calculate CRC32 checksum for the record
4. Serialize record (with checksum) using MessagePack
5. Write to current segment file
6. Add entry to `tmpIndex`

#### Rotation & Segment Management
When `tmpIndex` size exceeds `SegmentThreshold`:
1. Current segment is closed
2. `tmpIndex` is merged into main `index`
3. `tmpIndex` is cleared
4. New segment is created

When `MaxSegments` limit is reached, the oldest segment is automatically deleted along with its index entries to manage disk space.

#### Read Operations
Lookups check both indexes:
1. Check `tmpIndex` first (current segment, smaller and more likely to contain recent data)
2. If not found, check main `index` (historical segments)
3. Verify checksum before returning data

### Contributing
Feel free to open issues or submit pull requests for improvements and bug fixes. We welcome contributions!

### License
This project is licensed under the Apache License.