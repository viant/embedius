//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly || solaris || aix

package mmapstore

import (
	"golang.org/x/sys/unix"
)

// remap maps the segment file into memory read-only. If mapping fails, it is a no-op.
func (seg *segment) remap() error {
	// unmap previous mapping
	if seg.data != nil {
		_ = unix.Munmap(seg.data)
		seg.data = nil
	}
	if seg.size == 0 || seg.f == nil {
		return nil
	}
	// Map read-only; writes use file APIs; mapping is used only for reads.
	b, err := unix.Mmap(int(seg.f.Fd()), 0, int(seg.size), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		// ignore mapping errors; fallback path will use ReadAt
		return nil
	}
	seg.data = b
	return nil
}

// unmap releases any active mapping.
func (seg *segment) unmap() {
	if seg.data != nil {
		_ = unix.Munmap(seg.data)
		seg.data = nil
	}
}
