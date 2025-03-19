package retriever

type Source int

const (
	SourceUnknown Source = iota
	SourceFileStorage
)

type Location struct {
	Source    Source
	URL       string
	Extension []string
	Exclusion []string
}

func NewLocation(source Source, URL string, extension ...string) *Location {
	return &Location{
		Source:    source,
		URL:       URL,
		Extension: extension,
	}
}
