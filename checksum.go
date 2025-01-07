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
	fi, err := fd.Stat()
	if err != nil {
		return errors.Wrapf(err, "failed to get file info for log file %s", fi.Name())
	}

	chkFi, err := chk.Stat()
	if err != nil {
		return errors.Wrapf(err, "failed to get file info for checksum file %s", chkFi.Name())
	}

	if fi.Size() == 0 && chkFi.Size() == 0 {
		return nil
	}

	_, err = fd.Seek(0, io.SeekStart)
	if err != nil {
		return errors.Wrapf(err, "failed to seek to the beginning of the log file %s", fd.Name())
	}

	_, err = chk.Seek(0, io.SeekStart)
	if err != nil {
		return errors.Wrapf(err, "failed to seek to the beginning of the checksum file %s", chk.Name())
	}

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
	_, err := fd.Seek(0, 0)
	if err != nil {
		return errors.Wrap(err, "failed to seek to the beginning of the log file")
	}

	_, err = chk.Seek(0, 0)
	if err != nil {
		return errors.Wrap(err, "failed to seek to the beginning of the checksum file")
	}

	h := sha256.New()
	_, err = io.Copy(h, fd)
	if err != nil {
		return errors.Wrap(err, "failed to copy contents of segment file to calculate checksum")
	}

	sum := h.Sum(nil)

	// clear old checksum and write new
	err = chk.Truncate(0)
	if err != nil {
		return errors.Wrap(err, "failed to truncate checksum file")
	}

	_, err = chk.Write(sum)
	if err != nil {
		return errors.Wrap(err, "failed to write checksum to checksum file")
	}

	_, err = fd.Seek(0, io.SeekEnd)
	if err != nil {
		return errors.Wrap(err, "failed to restore original position in log file")
	}

	return nil
}
