package exchanges

import "time"

// splitEventTimestamp keeps timestamps compatible with downstream storage:
// datetimes are stored at microsecond precision, and residual nanoseconds live separately.
func splitEventTimestamp(timestamp time.Time) (time.Time, int) {
	timestamp = timestamp.UTC()
	return timestamp.Truncate(time.Microsecond), timestamp.Nanosecond() % int(time.Microsecond)
}
