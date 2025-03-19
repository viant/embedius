package indexer

type Option func(*Set)

func WithUpstreamURL(upstreamURL string) Option {
	return func(s *Set) {
		s.upstreamURL = upstreamURL
	}
}
