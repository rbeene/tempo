package activity

import (
	"bytes"
	"encoding/json"
	"math"
	"reflect"
	"testing"
	"time"
)

func TestSQLiteUint64CanonicalOrderAndBounds(t *testing.T) {
	values := []string{"0", "1", "9", "10", "9223372036854775807", "9223372036854775808", "18446744073709551615"}
	var previous []byte
	for _, value := range values {
		encoded, err := sqliteEncodeUint64(value)
		if err != nil {
			t.Fatalf("encode %s: %v", value, err)
		}
		decoded, err := sqliteDecodeUint64(encoded[:])
		if err != nil || decoded != value {
			t.Fatalf("roundtrip %s: %s, %v", value, decoded, err)
		}
		if previous != nil && bytes.Compare(previous, encoded[:]) >= 0 {
			t.Fatalf("counter ordering did not increase at %s", value)
		}
		previous = append([]byte(nil), encoded[:]...)
	}
	max, _ := sqliteEncodeUint64("18446744073709551615")
	if !bytes.Equal(max[:], bytes.Repeat([]byte{255}, 8)) {
		t.Fatal("MAX counter did not retain all unsigned bits")
	}
}

func TestSQLiteUint64RejectsMalformedValues(t *testing.T) {
	for _, value := range []string{"", "00", "01", "-1", "+1", " 1", "1 ", "1.0", "1e1", "18446744073709551616"} {
		if _, err := sqliteEncodeUint64(value); err == nil {
			t.Errorf("accepted noncanonical counter %q", value)
		}
	}
	for _, value := range [][]byte{nil, {}, {0}, make([]byte, 7), make([]byte, 9)} {
		if _, err := sqliteDecodeUint64(value); err == nil {
			t.Errorf("accepted counter width %d", len(value))
		}
	}
}

func TestSQLiteSignedValuesRetainResiduals(t *testing.T) {
	for _, value := range []string{"-9223372036854775808", "-1", "0", "1", "9223372036854775807"} {
		encoded, err := sqliteEncodeInt64(value)
		if err != nil || sqliteDecodeInt64(encoded) != value {
			t.Fatalf("signed roundtrip %q: %d, %v", value, encoded, err)
		}
	}
	for _, value := range []string{"", "-0", "00", "+1", "01", "1.0", "1e1", " 1", "9223372036854775808", "-9223372036854775809"} {
		if _, err := sqliteEncodeInt64(value); err == nil {
			t.Errorf("accepted noncanonical signed value %q", value)
		}
	}
}

func TestSQLiteTimeRetainsLegacyJSONAndParsing(t *testing.T) {
	values := []string{
		`"0000-01-01T00:00:00Z"`,
		`"0001-01-01T00:00:00Z"`,
		`"1969-12-31T23:59:59.999999999Z"`,
		`"1970-01-01T00:00:00Z"`,
		`"2026-10-03T12:34:56.123456789+05:45"`,
		`"2026-10-03T12:34:56-07:00"`,
		`"9999-12-31T23:59:59.999999999Z"`,
	}
	for _, text := range values {
		var legacy time.Time
		if err := legacy.UnmarshalJSON([]byte(text)); err != nil {
			t.Fatalf("legacy fixture %s: %v", text, err)
		}
		encoded, err := sqliteEncodeTime(legacy)
		if err != nil {
			t.Fatalf("encode %s: %v", text, err)
		}
		decoded, err := sqliteDecodeTime(encoded)
		if err != nil || !reflect.DeepEqual(legacy, decoded) {
			t.Fatalf("legacy time identity %s: %v", text, err)
		}
		got, _ := decoded.MarshalJSON()
		want, _ := legacy.MarshalJSON()
		if !bytes.Equal(got, want) || encoded.Seconds != legacy.Unix() || encoded.Nanoseconds != int64(legacy.Nanosecond()) {
			t.Fatalf("time JSON or coordinate changed for %s", text)
		}
	}
}

func TestSQLiteTimeCanonicalizesExactlyLikeLegacyWrite(t *testing.T) {
	input := time.Date(2026, 10, 3, 12, 0, 0, 12, time.FixedZone("synthetic", 1234))
	legacyJSON, err := input.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	var legacy time.Time
	if err := legacy.UnmarshalJSON(legacyJSON); err != nil {
		t.Fatal(err)
	}
	encoded, err := sqliteEncodeTime(input)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := sqliteDecodeTime(encoded)
	if err != nil || !reflect.DeepEqual(decoded, legacy) {
		t.Fatalf("codec differs from legacy JSON parse: %v", err)
	}
}

func TestSQLiteTimeRejectsMalformedOrMismatchedCoordinates(t *testing.T) {
	valid, err := sqliteEncodeTime(time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	cases := []sqliteTimeValue{valid, valid, valid, valid, valid, valid, valid}
	cases[0].Seconds++
	cases[1].Nanoseconds = -1
	cases[2].Nanoseconds = 1000000000
	cases[3].JSON = "null"
	cases[4].JSON = `"2026-10-03T00:00:00.000Z"`
	cases[5].JSON = `"2026-10-03T00:00:00Z" true`
	cases[6].Seconds = math.MaxInt64
	for i, value := range cases {
		if _, err := sqliteDecodeTime(value); err == nil {
			t.Errorf("accepted invalid time fixture %d", i)
		}
	}
	for _, value := range []time.Time{
		time.Date(-1, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 1, 1, 0, 0, 0, 0, time.FixedZone("invalid", 24*60*60)),
	} {
		if _, err := sqliteEncodeTime(value); err == nil {
			t.Error("accepted a time outside legacy JSON encoding")
		}
	}
}

func TestSQLiteOptionalTimePreservesNullAndZero(t *testing.T) {
	encoded, err := sqliteEncodeOptionalTime(nil)
	if err != nil || encoded != nil {
		t.Fatal("nil time became a value")
	}
	decoded, err := sqliteDecodeOptionalTime(nil)
	if err != nil || decoded != nil {
		t.Fatal("NULL time became a value")
	}
	zero := time.Time{}
	encoded, err = sqliteEncodeOptionalTime(&zero)
	if err != nil || encoded == nil {
		t.Fatal("present zero time became NULL")
	}
	decoded, err = sqliteDecodeOptionalTime(encoded)
	if err != nil || decoded == nil || !decoded.IsZero() {
		t.Fatal("present zero time did not roundtrip")
	}
}

func TestSQLiteDetectionPreservesPermissiveClockFields(t *testing.T) {
	epoch, elapsed, awake := "", "001", "18446744073709551616"
	input := ClockSample{Capability: "legacy-unrecognized", WallUTC: time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC), Epoch: &epoch, ElapsedNS: &elapsed, AwakeNS: &awake}
	encoded, err := sqliteEncodeClock(input, false)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := sqliteDecodeClock(encoded)
	if err != nil || !reflect.DeepEqual(input, decoded) || encoded.ElapsedCounter != nil || encoded.AwakeCounter != nil {
		t.Fatalf("permissive evidence changed: %v", err)
	}
	elapsed = "changed"
	if *encoded.ElapsedRaw != "001" {
		t.Fatal("prepared clock aliases caller-owned pointer")
	}
	for _, value := range []ClockSample{
		{Capability: "", WallUTC: input.WallUTC},
		{Capability: "available", WallUTC: input.WallUTC, ElapsedNS: &awake},
	} {
		encoded, err := sqliteEncodeClock(value, false)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := sqliteDecodeClock(encoded)
		if err != nil || !reflect.DeepEqual(value, decoded) {
			t.Fatal("detection pointer presence or raw strings changed")
		}
	}
}

func TestSQLiteRawClockRetainsZeroForDiscardedRecovery(t *testing.T) {
	// This is an admitted unchecked ObservedSample on a discarded decision,
	// not a valid Detection sample. Field-level validation retains that distinction.
	encoded, err := sqliteEncodeClock(ClockSample{}, false)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := sqliteDecodeClock(encoded)
	if err != nil || !reflect.DeepEqual(decoded, ClockSample{}) {
		t.Fatal("raw clock tightened the discarded-recovery contract")
	}
	if _, err := sqliteEncodeClock(ClockSample{}, true); err == nil {
		t.Fatal("zero clock admitted as available evidence")
	}
}

func TestSQLiteClockPreparationMatchesLegacyMalformedUTF8Persistence(t *testing.T) {
	malformed := string([]byte{0xff, 0xfe})
	wall := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	epoch, elapsed, awake := "boot"+malformed, "2", "1"
	rawEpoch, rawElapsed, rawAwake := "epoch"+malformed, "001"+malformed, malformed
	cases := []struct {
		name      string
		input     ClockSample
		available bool
	}{
		{"available_epoch", ClockSample{Capability: "available", WallUTC: wall, Epoch: &epoch, ElapsedNS: &elapsed, AwakeNS: &awake}, true},
		{"permissive_detection", ClockSample{Capability: "legacy" + malformed, WallUTC: wall, Epoch: &rawEpoch, ElapsedNS: &rawElapsed, AwakeNS: &rawAwake}, false},
		{"discarded_recovery_zero_wall", ClockSample{Capability: malformed, Epoch: &rawEpoch, ElapsedNS: &rawElapsed}, false},
	}
	for _, fixture := range cases {
		t.Run(fixture.name, func(t *testing.T) {
			if fixture.available {
				if _, _, ok := sampleValues(fixture.input); !ok {
					t.Fatal("fixture must pass the actual pre-persistence evidence validator")
				}
			}
			// This is the independent old file-store persistence oracle. It
			// replaces malformed Go UTF-8 with U+FFFD before the next read.
			persisted, err := json.Marshal(fixture.input)
			if err != nil {
				t.Fatal(err)
			}
			var expected ClockSample
			if err := json.Unmarshal(persisted, &expected); err != nil {
				t.Fatal(err)
			}
			if reflect.DeepEqual(fixture.input, expected) {
				t.Fatal("fixture did not exercise legacy UTF-8 replacement")
			}
			encoded, err := sqliteEncodeClock(fixture.input, fixture.available)
			if err != nil {
				t.Fatal(err)
			}
			actual, err := sqliteDecodeClock(encoded)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(actual, expected) {
				t.Fatal("SQL clock preparation differs from the legacy JSON persistence boundary")
			}
		})
	}
}

func TestSQLiteAvailableClockRequiresExistingEvidenceContract(t *testing.T) {
	epoch, elapsed, awake := "boot", "9223372036854775807", "0"
	input := ClockSample{Capability: "available", WallUTC: time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC), Epoch: &epoch, ElapsedNS: &elapsed, AwakeNS: &awake}
	encoded, err := sqliteEncodeClock(input, true)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := sqliteDecodeClock(encoded)
	if err != nil || !reflect.DeepEqual(input, decoded) {
		t.Fatalf("available evidence roundtrip: %v", err)
	}
	bad := encoded
	bad.ElapsedCounter = make([]byte, 8)
	if _, err := sqliteDecodeClock(bad); err == nil {
		t.Fatal("accepted counter projection that disagrees with retained string")
	}
	bad = encoded
	bad.AwakeCounter = []byte{0}
	if _, err := sqliteDecodeClock(bad); err == nil {
		t.Fatal("accepted malformed clock counter projection")
	}
	for _, value := range []string{"9223372036854775808", "01", "-1"} {
		copy := input
		copy.ElapsedNS = &value
		if _, err := sqliteEncodeClock(copy, true); err == nil {
			t.Errorf("accepted inadmissible available evidence %q", value)
		}
	}
	input.Epoch = nil
	if _, err := sqliteEncodeClock(input, true); err == nil {
		t.Fatal("accepted available evidence without epoch")
	}
}

func TestSQLiteReplayNonceChangesAndWrapsWithoutPublicCounter(t *testing.T) {
	zero := make([]byte, 16)
	next, err := sqliteNextNonce(zero)
	if err != nil || next[15] != 1 || !bytes.Equal(zero, make([]byte, 16)) {
		t.Fatal("nonce increment failed or mutated input")
	}
	max := bytes.Repeat([]byte{255}, 16)
	wrapped, err := sqliteNextNonce(max)
	if err != nil || !bytes.Equal(wrapped[:], zero) || !bytes.Equal(max, bytes.Repeat([]byte{255}, 16)) {
		t.Fatal("128-bit private nonce did not wrap independently")
	}
	carry := make([]byte, 16)
	carry[15] = 255
	next, err = sqliteNextNonce(carry)
	if err != nil || next[14] != 1 || next[15] != 0 {
		t.Fatal("nonce carry failed")
	}
	for _, value := range [][]byte{nil, make([]byte, 15), make([]byte, 17)} {
		if _, err := sqliteNextNonce(value); err == nil {
			t.Errorf("accepted nonce width %d", len(value))
		}
	}
}
