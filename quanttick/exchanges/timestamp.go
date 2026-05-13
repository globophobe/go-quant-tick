package exchanges

import "time"

// splitEventTimestamp keeps timestamps compatible with DQT storage: Django
// stores microsecond datetimes, and the residual nanoseconds live separately.
func splitEventTimestamp(timestamp time.Time) (time.Time, int) {
	timestamp = timestamp.UTC()
	return timestamp.Truncate(time.Microsecond), timestamp.Nanosecond() % int(time.Microsecond)
}
