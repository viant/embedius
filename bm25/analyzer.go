package bm25

import (
	sb "github.com/blevesearch/snowballstem"
	"github.com/blevesearch/snowballstem/english"
	"github.com/clipperhouse/uax29/v2/words"
	"strings"
	"unicode"
)

func analyzer(stoplist []string, transliterate func(string) string) func(string) []string {
	stopwords := make(map[string]struct{}, len(stoplist))
	for _, word := range stoplist {
		stopwords[transliterate(word)] = struct{}{}
	}
	return func(input string) []string {
		text := strings.ToLower(transliterate(input))
		iter := words.FromString(text)
		var result []string
		env := sb.NewEnv("")
		for iter.Next() {
			word := iter.Value()
			if !strings.ContainsFunc(word, func(r rune) bool { return unicode.IsLetter(r) || unicode.IsNumber(r) }) {
				continue
			}
			if _, excluded := stopwords[word]; excluded {
				continue
			}
			env.SetCurrent(word)
			english.Stem(env)
			result = append(result, env.Current())
		}
		return result
	}
}

var defaultAnalyzer = analyzer(englishStopwords, deunicode)
