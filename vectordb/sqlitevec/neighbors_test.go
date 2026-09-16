package sqlitevec

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/viant/embedius/schema"
)

func TestExpandNeighborFragments(t *testing.T) {
	store, err := NewStore(WithDSN(":memory:"), WithEnsureSchema(true))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.DB().Close() })

	for index, content := range []string{"before", "anchor", "after", "later"} {
		start := index * 10
		id := "article.md:" + itoa(start) + "-" + itoa(start+10)
		metadata, err := json.Marshal(map[string]interface{}{
			"path":       "article.md",
			"docId":      "article.md",
			"fragmentId": id,
			"start":      start,
			"end":        start + 10,
		})
		if err != nil {
			t.Fatal(err)
		}
		_, err = store.DB().Exec(`INSERT INTO _vec_emb_docs(dataset_id,id,asset_id,content,meta,embedding,embedding_model,scn,archived) VALUES(?,?,?,?,?,?,?,?,0)`, "docs", id, id, content, string(metadata), []byte{}, "test", index+1)
		if err != nil {
			t.Fatal(err)
		}
	}

	anchor := schema.Document{
		PageContent: "anchor",
		Score:       0.9,
		Metadata: map[string]interface{}{
			"path":       "article.md",
			"fragmentId": "article.md:10-20",
			"start":      float64(10),
		},
	}
	got, err := store.expandNeighborFragments(context.Background(), "docs", []schema.Document{anchor}, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d fragments, want 3", len(got))
	}
	if contents := []string{got[0].PageContent, got[1].PageContent, got[2].PageContent}; !reflect.DeepEqual(contents, []string{"before", "anchor", "after"}) {
		t.Fatalf("contents = %#v", contents)
	}
	for index := range got {
		if got[index].Score != float32(0.9) {
			t.Fatalf("score[%d] = %v", index, got[index].Score)
		}
	}
	if got[0].Metadata["neighborOffset"] != -1 {
		t.Fatalf("before offset = %#v", got[0].Metadata["neighborOffset"])
	}
	if got[2].Metadata["neighborOffset"] != 1 {
		t.Fatalf("after offset = %#v", got[2].Metadata["neighborOffset"])
	}
}

func itoa(value int) string {
	if value == 0 {
		return "0"
	}
	var buf [20]byte
	position := len(buf)
	for value > 0 {
		position--
		buf[position] = byte('0' + value%10)
		value /= 10
	}
	return string(buf[position:])
}
