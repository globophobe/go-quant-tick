package exchanges

type seenTradeIDs struct {
	limit int
	seen  map[string]struct{}
	order []string
}

func newSeenTradeIDs(limit int) *seenTradeIDs {
	return &seenTradeIDs{
		limit: limit,
		seen:  make(map[string]struct{}),
	}
}

func (s *seenTradeIDs) Add(symbol, uid string) bool {
	key := symbol + "|" + uid
	if _, ok := s.seen[key]; ok {
		return false
	}
	s.seen[key] = struct{}{}
	s.order = append(s.order, key)
	for s.limit > 0 && len(s.order) > s.limit {
		oldest := s.order[0]
		s.order = s.order[1:]
		delete(s.seen, oldest)
	}
	return true
}
