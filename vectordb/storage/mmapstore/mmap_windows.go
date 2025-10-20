//go:build windows

package mmapstore

// On Windows, provide no-op mmap to keep builds portable.
// Reads fall back to direct file I/O via ReadAt in store.go.

func (seg *segment) remap() error {
	// Disable mmap on Windows for now
	seg.data = nil
	return nil
}

func (seg *segment) unmap() {
	// Nothing to do
}
