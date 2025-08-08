package gowal

import (
	"encoding/binary"
	"fmt"
	"github.com/pkg/errors"
	"hash/crc32"
	"io"
	"os"
)

const checkSumPostfix = ".checksum"

func compareChecksums(fd *os.File, chk *os.File) error {
	fd, err := os.Open(fd.Name())
	if err != nil {
		return errors.Wrap(err, "failed to open segment file")
	}
	defer fd.Close()

	chk, err = os.Open(chk.Name())
	if err != nil {
		return errors.Wrap(err, "failed to open segment checksum file")
	}
	defer chk.Close()

	h := crc32.NewIEEE()
	_, err = io.Copy(h, fd)
	if err != nil {
		return errors.Wrapf(err, "failed to copy contents of segment file %s to verify checksum", fd.Name())
	}

	expectedSum := h.Sum32()

	buf := make([]byte, 4)
	_, err = io.ReadFull(chk, buf)
	if err != nil {
		return errors.Wrapf(err, "failed to read checksum file %s", chk.Name())
	}

	actualSum := binary.BigEndian.Uint32(buf)
	if expectedSum != actualSum {
		return fmt.Errorf("file %s corrupted, checksums do not match, expected %x, got %x", fd.Name(), expectedSum, actualSum)
	}

	return nil
}

func writeChecksum(fd *os.File, chk *os.File) error {
	fd, err := os.Open(fd.Name())
	if err != nil {
		return errors.Wrap(err, "failed to open segment file")
	}
	defer fd.Close()

	h := crc32.NewIEEE()
	_, err = io.Copy(h, fd)
	if err != nil {
		return errors.Wrap(err, "failed to copy contents of segment file to calculate checksum")
	}

	sum := h.Sum32()

	chk, err = os.OpenFile(chk.Name(), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return errors.Wrap(err, "failed to create new log file")
	}
	defer chk.Close()

	err = chk.Truncate(0)
	if err != nil {
		return errors.Wrap(err, "failed to truncate checksum file")
	}

	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, sum)
	_, err = chk.Write(buf)
	if err != nil {
		return errors.Wrap(err, "failed to write checksum to checksum file")
	}

	return nil
}
