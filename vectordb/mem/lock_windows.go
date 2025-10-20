//go:build windows

package mem

import (
	"errors"
	"os"

	"golang.org/x/sys/windows"
)

var errWouldBlock = errors.New("would block")

func lockExclusiveBlocking(f *os.File) error {
	h := windows.Handle(f.Fd())
	var ol windows.Overlapped
	// Lock the first byte; this is a common pattern for file-based mutexes.
	return windows.LockFileEx(h, windows.LOCKFILE_EXCLUSIVE_LOCK, 0, 1, 0, &ol)
}

func tryLockExclusive(f *os.File) error {
	h := windows.Handle(f.Fd())
	var ol windows.Overlapped
	err := windows.LockFileEx(h, windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, &ol)
	if err != nil {
		if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
			return errWouldBlock
		}
		return err
	}
	return nil
}

func unlockFile(f *os.File) error {
	h := windows.Handle(f.Fd())
	var ol windows.Overlapped
	return windows.UnlockFileEx(h, 0, 1, 0, &ol)
}
