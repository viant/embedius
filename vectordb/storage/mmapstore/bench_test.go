package mmapstore

import (
	"math/rand"
	"testing"
	"time"

	vstorage "github.com/viant/embedius/vectordb/storage"
)

func tmpDirTB(tb testing.TB) string {
	tb.Helper()
	return tb.TempDir()
}

func BenchmarkAppend_Sizes(b *testing.B) {
	sizes := []int{256, 1024, 4096, 16384}
	for _, sz := range sizes {
		b.Run("size_"+itoa(sz), func(b *testing.B) {
			base := tmpDirTB(b)
			s, err := Open(Options{BasePath: base, SegmentSize: 256 << 20}) // 256MiB
			if err != nil {
				b.Fatalf("open: %v", err)
			}
			defer s.Close()
			payload := make([]byte, sz)
			rand.Read(payload)

			b.SetBytes(int64(sz))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := s.Append(payload); err != nil {
					b.Fatalf("append: %v", err)
				}
			}
			b.StopTimer()
		})
	}
}

func BenchmarkRead_Parallel(b *testing.B) {
	base := tmpDirTB(b)
	s, err := Open(Options{BasePath: base, SegmentSize: 256 << 20})
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer s.Close()

	// prepare records
	const recs = 10000
	ptrs := make([]vstorage.Ptr, recs)
	payload := make([]byte, 1024)
	rand.Read(payload)
	for i := 0; i < recs; i++ {
		p, err := s.Append(payload)
		if err != nil {
			b.Fatalf("append: %v", err)
		}
		ptrs[i] = p
	}
	// Optional: sync to simulate steady state
	_ = s.Sync()

	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		for pb.Next() {
			p := ptrs[rng.Intn(len(ptrs))]
			if _, err := s.Read(p); err != nil {
				b.Fatalf("read: %v", err)
			}
		}
	})
}

// small itoa without strconv import for benchmarks
func itoa(v int) string {
	return string([]byte{'0' + byte(v/10000%10), '0' + byte(v/1000%10), '0' + byte(v/100%10), '0' + byte(v/10%10), '0' + byte(v%10)})
}
