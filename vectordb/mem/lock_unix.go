//go:build !windows

package mem

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

var errWouldBlock = errors.New("would block")

func lockExclusiveBlocking(f *os.File) error {
	return unix.Flock(int(f.Fd()), unix.LOCK_EX)
}

func tryLockExclusive(f *os.File) error {
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return errWouldBlock
		}
		return err
	}
	return nil
}

func unlockFile(f *os.File) error {
	return unix.Flock(int(f.Fd()), unix.LOCK_UN)
}
