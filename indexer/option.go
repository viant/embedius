package indexer

type Option func(*Set)

// WithUpstreamURL sets the upstream URL
func WithUpstreamURL(upstreamURL string) Option {
	return func(s *Set) {
		s.upstreamURL = upstreamURL
	}
}
