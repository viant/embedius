package mem

type StoreOption func(s *Store)

func WithBaseURL(baseURL string) StoreOption {
	return func(s *Store) {
		s.baseURL = baseURL
	}
}

type SetOption func(s *Set)
