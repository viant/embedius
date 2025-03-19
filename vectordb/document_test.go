package vectordb

import (
	"fmt"
	"github.com/viant/bintly"
	"testing"
	"time"
)

func TestDocument_EncodeDecodeBinary(t *testing.T) {
	ts := time.Now()
	original := &Document{
		PageContent: "Test content",
		Metadata: map[string]interface{}{
			"intKey":    42,
			"floatKey":  3.14,
			"stringKey": "value",
			"timeKey":   ts,
		},
	}

	writers := bintly.NewWriters()
	writer := writers.Get()
	defer writers.Put(writer)

	err := original.EncodeBinary(writer)
	if err != nil {
		t.Fatalf("EncodeBinary failed: %v", err)
	}

	bs := writer.Bytes()
	fmt.Println(len(bs))
	readers := bintly.NewReaders()
	reader := readers.Get()
	_ = reader.FromBytes(bs)
	defer readers.Put(reader)

	decoded := &Document{}
	err = decoded.DecodeBinary(reader)
	if err != nil {
		t.Fatalf("DecodeBinary failed: %v", err)
	}

	if original.PageContent != decoded.PageContent {
		t.Errorf("PageContent mismatch: got %v, want %v", decoded.PageContent, original.PageContent)
	}

	for key, value := range original.Metadata {
		if key == "timeKey" { //custom time valudation
			cloned := decoded.Metadata[key].(time.Time)
			source := value.(time.Time)
			if !cloned.Equal(source) {
				t.Errorf("Metadata mismatch for key %v: got %v, want %v", key, cloned.Unix(), source.Unix())
			}
			continue
		}

		if decoded.Metadata[key] != value {
			t.Errorf("Metadata mismatch for key %v: got %v, want %v", key, decoded.Metadata[key], value)
		}
	}
}
