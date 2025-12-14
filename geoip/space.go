package geoip

// SpacedSet is a set of hosts that are sufficiently spaced apart based on a
// minimum distance. It is not thread-safe.
type SpacedSet struct {
	minDistanceKm float64
	seen          map[Location]struct{}
}

// NewSpacedSet creates a new SpacedSet with the given minimum distance.
func NewSpacedSet(minDistanceKm float64) *SpacedSet {
	return &SpacedSet{
		minDistanceKm: minDistanceKm,
	}
}

// Add adds the locations to the set if all locations are sufficiently spaced apart
// from existing locations. It returns true only if all the locations were valid, and false
// otherwise.
func (s *SpacedSet) Add(locs []Location) bool {
	for existingLoc := range s.seen {
		for _, loc := range locs {
			distance := loc.HaversineDistanceKm(existingLoc)
			if distance < s.minDistanceKm {
				return false
			}
		}
	}
	for _, loc := range locs {
		s.seen[loc] = struct{}{}
	}
	return true
}
