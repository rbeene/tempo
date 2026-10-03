package activity

import (
	"bytes"
	"encoding/binary"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// Counters remain unsigned across the SQL boundary. Fixed-width big endian
// BLOBs have the same ordering as the existing canonical decimal uint64 domain.
func sqliteEncodeUint64(value string) ([8]byte, error) {
	var encoded [8]byte
	n, ok := counter(value)
	if !ok {
		return encoded, failure("state_corrupt")
	}
	binary.BigEndian.PutUint64(encoded[:], n)
	return encoded, nil
}

func sqliteDecodeUint64(value []byte) (string, error) {
	if len(value) != 8 {
		return "", failure("state_corrupt")
	}
	return strconv.FormatUint(binary.BigEndian.Uint64(value), 10), nil
}

func sqliteEncodeInt64(value string) (int64, error) {
	n, ok := syncInt(value)
	if !ok {
		return 0, failure("state_corrupt")
	}
	return n, nil
}

func sqliteDecodeInt64(value int64) string { return strconv.FormatInt(value, 10) }

// Each SQL wall time has indexed seconds/nanoseconds and its canonical legacy
// JSON representation. JSON retains offsets and uses time.Time's original parse
// semantics, including its UTC/Local/fixed-zone choice. No UnixNano is involved.
type sqliteTimeValue struct {
	Seconds     int64
	Nanoseconds int64
	JSON        string
}

func sqliteEncodeTime(value time.Time) (sqliteTimeValue, error) {
	text, err := value.MarshalJSON()
	if err != nil {
		return sqliteTimeValue{}, failure("state_corrupt")
	}
	var canonical time.Time
	if canonical.UnmarshalJSON(text) != nil {
		return sqliteTimeValue{}, failure("state_corrupt")
	}
	// Match the old store's write/read boundary, including a fixed zone whose
	// sub-minute offset cannot be represented by the legacy RFC3339 encoding.
	return sqliteTimeValue{Seconds: canonical.Unix(), Nanoseconds: int64(canonical.Nanosecond()), JSON: string(text)}, nil
}

func sqliteDecodeTime(value sqliteTimeValue) (time.Time, error) {
	if value.Nanoseconds < 0 || value.Nanoseconds >= 1000000000 {
		return time.Time{}, failure("state_corrupt")
	}
	var decoded time.Time
	if decoded.UnmarshalJSON([]byte(value.JSON)) != nil || decoded.Unix() != value.Seconds || int64(decoded.Nanosecond()) != value.Nanoseconds {
		return time.Time{}, failure("state_corrupt")
	}
	canonical, err := decoded.MarshalJSON()
	if err != nil || string(canonical) != value.JSON {
		return time.Time{}, failure("state_corrupt")
	}
	return decoded, nil
}

// An optional time maps to three all-NULL or all-present columns. A present
// zero time remains present; nonzero requirements belong to its domain field.
func sqliteEncodeOptionalTime(value *time.Time) (*sqliteTimeValue, error) {
	if value == nil {
		return nil, nil
	}
	encoded, err := sqliteEncodeTime(*value)
	if err != nil {
		return nil, err
	}
	return &encoded, nil
}

func sqliteDecodeOptionalTime(value *sqliteTimeValue) (*time.Time, error) {
	if value == nil {
		return nil, nil
	}
	decoded, err := sqliteDecodeTime(*value)
	if err != nil {
		return nil, err
	}
	return &decoded, nil
}

// This value expands into inline columns on the owning domain row. Available
// samples retain both exact strings and tested numeric projections. Detection
// evidence has only the raw fields. Its domain validator separately requires
// nonzero wall time. A discarded recovery decision may retain an unchecked
// observed sample, including zero wall time, so raw encoding does not strengthen
// that distinct legacy contract.
type sqliteClockValue struct {
	Available      bool
	Capability     string
	Wall           sqliteTimeValue
	Epoch          *string
	ElapsedRaw     *string
	AwakeRaw       *string
	ElapsedCounter []byte
	AwakeCounter   []byte
}

func sqliteEncodeClock(value ClockSample, available bool) (sqliteClockValue, error) {
	if available {
		if _, _, ok := sampleValues(value); !ok {
			return sqliteClockValue{}, failure("state_corrupt")
		}
	}
	wall, err := sqliteEncodeTime(value.WallUTC)
	if err != nil {
		return sqliteClockValue{}, err
	}
	// This is final persistence encoding, after raw admission/reducer decisions.
	// Match legacy JSON's replacement of each invalid UTF-8 byte. The old store
	// checks epoch byte length before serialization; replacement can expand it
	// past that limit. Do not add a new revalidation policy at this boundary.
	encoded := sqliteClockValue{Available: available, Capability: sqlitePersistClockString(value.Capability), Wall: wall, Epoch: sqlitePersistClockOptional(value.Epoch), ElapsedRaw: sqlitePersistClockOptional(value.ElapsedNS), AwakeRaw: sqlitePersistClockOptional(value.AwakeNS)}
	if available {
		elapsed, _ := sqliteEncodeUint64(*value.ElapsedNS)
		awake, _ := sqliteEncodeUint64(*value.AwakeNS)
		encoded.ElapsedCounter = elapsed[:]
		encoded.AwakeCounter = awake[:]
	}
	return encoded, nil
}

func sqlitePersistClockString(value string) string {
	if utf8.ValidString(value) {
		return value
	}
	var encoded strings.Builder
	encoded.Grow(len(value))
	for _, r := range value {
		encoded.WriteRune(r)
	}
	return encoded.String()
}

func sqlitePersistClockOptional(value *string) *string {
	encoded := sqliteCopyString(value)
	if encoded != nil {
		*encoded = sqlitePersistClockString(*encoded)
	}
	return encoded
}

func sqliteDecodeClock(value sqliteClockValue) (ClockSample, error) {
	wall, err := sqliteDecodeTime(value.Wall)
	if err != nil {
		return ClockSample{}, failure("state_corrupt")
	}
	decoded := ClockSample{Capability: value.Capability, WallUTC: wall, Epoch: sqliteCopyString(value.Epoch), ElapsedNS: sqliteCopyString(value.ElapsedRaw), AwakeNS: sqliteCopyString(value.AwakeRaw)}
	if !value.Available {
		if value.ElapsedCounter != nil || value.AwakeCounter != nil {
			return ClockSample{}, failure("state_corrupt")
		}
		return decoded, nil
	}
	if _, _, ok := sampleValues(decoded); !ok {
		return ClockSample{}, failure("state_corrupt")
	}
	elapsed, _ := sqliteEncodeUint64(*decoded.ElapsedNS)
	awake, _ := sqliteEncodeUint64(*decoded.AwakeNS)
	if !bytes.Equal(value.ElapsedCounter, elapsed[:]) || !bytes.Equal(value.AwakeCounter, awake[:]) {
		return ClockSample{}, failure("state_corrupt")
	}
	return decoded, nil
}

func sqliteCopyString(value *string) *string {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

// The private replay fence always changes its 16-byte value, including wrap.
// It is independent of all public counters and never aliases caller memory.
func sqliteNextNonce(value []byte) ([16]byte, error) {
	var next [16]byte
	if len(value) != len(next) {
		return next, failure("state_corrupt")
	}
	copy(next[:], value)
	for i := len(next) - 1; i >= 0; i-- {
		next[i]++
		if next[i] != 0 {
			break
		}
	}
	return next, nil
}
