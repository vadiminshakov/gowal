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
You can append a new log entry by providing an index, a key, and a value. Indexes start at `1` and must increase monotonically with each write:
```go
err := wal.Write(1, "myKey", []byte("myValue"))
if err != nil {
    log.Fatal(err)
}
```
If the index does not continue the monotonic sequence, the function will return an error.

The WAL is fail-stop: if writing or syncing the log fails, the write returns `ErrFailed` and all further writes return it too.
The in-memory state keeps only the records written before the error. Close the WAL and open it again with `NewWAL`:
the failed record may or may not be in the reopened log. A record that is too large (`ErrRecordTooLarge`) does not stop the WAL.
Neither does a failure to create a new segment on rotation: nothing is written in this case, and the write can be retried.

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
for record := range wal.Iterator() {
    log.Printf("Key: %s, Value: %s\n", record.Key, string(record.Value))
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
A torn last frame left by a crash in the middle of a write is truncated by `NewWAL` automatically.
Corruption anywhere else or a gap in indexes (`ErrIndexGap`, e.g. a segment file from the middle is lost) makes `NewWAL` return an error,
because the records after it are valid and dropping them loses data.

If you can't restore the log from elsewhere, you can cut it to its valid prefix by calling the `UnsafeRecover` function.
It truncates the log at the first broken frame or index gap and removes everything after it, including all later segments:

```go
result, err := gowal.UnsafeRecover("./log", "segment_")
if err != nil {
    log.Fatal(err)
}
log.Printf("Truncated: %q, removed: %v, last index: %d", result.Truncated, result.Removed, result.LastIndex)
```

### Configuration
The behavior of the WAL can be configured using several configuration options (`Config` parameter in the `NewWAL` function):

 - `SegmentThreshold`: Maximum number of log entries per segment before rotation occurs. Default is 1000.
 - `MaxSegments`: Maximum number of segments to keep before the oldest segments are deleted. Default is 5.
 - `IsInSyncDiskMode`: When set to true, every write is synced to disk, ensuring durability at the cost of performance. Default is false.

## Architecture

GoWAL uses a segmented architecture with segment-owned indexing for efficient write and read operations:

#### Segments
Data is split into numbered files (`segment_0`, `segment_1`, etc.). Internally, each active segment owns its file descriptor and in-memory index. Each record contains:
- Index, Key, Value
- CRC32 checksum for integrity verification

#### Segment Indexing
- **active segment index**: In-memory index owned by the current active segment.
- **historical index**: Main in-memory index for all closed segments. Provides fast lookups across historical data.

#### Write Flow
1. Check that the index continues the monotonic sequence
2. Check if rotation is needed based on `SegmentThreshold`
3. Calculate CRC32 checksum for the record
4. Serialize record (with checksum) using MessagePack
5. Write to current segment file
6. Add entry to the active segment index

#### Rotation & Segment Management
When the active segment size exceeds `SegmentThreshold`:
1. Current segment is synced
2. New segment is created; if it fails, the current segment stays active
3. Active segment index is merged into the historical index and the new segment becomes active
4. Old segment is closed

When `MaxSegments` limit is reached, the oldest segment is automatically deleted along with its index entries to manage disk space.
A failed deletion does not fail the write: the segment is kept and deleted on the next rotation.

#### Read Operations
Lookups check both indexes:
1. Check the active segment index first (current segment, smaller and more likely to contain recent data)
2. If not found, check the historical index
3. Verify checksum before returning data

### Contributing
Feel free to open issues or submit pull requests for improvements and bug fixes. We welcome contributions!

### License
This project is licensed under the Apache License.