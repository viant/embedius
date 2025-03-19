package cache

import (
	"github.com/minio/highwayhash"
)

var key = []byte("0123456789ABCDEF0123456789ABCDEF")

// Hash creates a hash for the input data
func Hash(data []byte) (uint64, error) {
	h, err := highwayhash.New64(key)
	if err != nil {
		return 0, err
	}
	_, err = h.Write(data)
	if err != nil {
		return 0, err
	}
	return h.Sum64(), nil
}
