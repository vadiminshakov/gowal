package gowal

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"github.com/pkg/errors"
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

	h := sha256.New()
	_, err = io.Copy(h, fd)
	if err != nil {
		return errors.Wrapf(err, "failed to copy contents of segment file %s to verify checksum", fd.Name())
	}

	sum := h.Sum(nil)

	buf, err := io.ReadAll(chk)
	if err != nil {
		return errors.Wrapf(err, "failed to read checksum file %s", chk.Name())
	}

	if !bytes.Equal(sum, buf) {
		return fmt.Errorf("file %s corrupted, checksums do not match, expected %x, got %x", fd.Name(), sum[len(sum)-5:], buf[len(buf)-5:])
	}

	return nil
}

func writeChecksum(fd *os.File, chk *os.File) error {
	fd, err := os.Open(fd.Name())
	if err != nil {
		return errors.Wrap(err, "failed to open segment file")
	}
	defer fd.Close()

	h := sha256.New()
	_, err = io.Copy(h, fd)
	if err != nil {
		return errors.Wrap(err, "failed to copy contents of segment file to calculate checksum")
	}

	sum := h.Sum(nil)

	chk, err = os.OpenFile(chk.Name(), os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return errors.Wrap(err, "failed to create new log file")
	}
	defer chk.Close()

	err = chk.Truncate(0)
	if err != nil {
		return errors.Wrap(err, "failed to truncate checksum file")
	}

	//_, err = chk.Seek(0, io.SeekStart)
	//if err != nil {
	//	return errors.Wrap(err, "failed to seek to start of checksum file")
	//}

	_, err = chk.Write(sum)
	if err != nil {
		return errors.Wrap(err, "failed to write checksum to checksum file")
	}

	return nil
}
