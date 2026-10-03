//go:build (darwin || linux) && (amd64 || arm64)

package activity

import "math"

const sqliteLogicalCapacity int64 = 64 << 20

// sqliteRowCharge measures retained values, not SQLite pages or index copies.
func sqliteRowCharge(integerCount, nullCount int, texts []string, blobs [][]byte) (int64, error) {
	if integerCount < 0 || nullCount < 0 || int64(integerCount) > (math.MaxInt64-32)/9 {
		return 0, failure("state_corrupt")
	}
	charge := int64(32) + int64(integerCount)*9
	if int64(nullCount) > math.MaxInt64-charge {
		return 0, failure("state_corrupt")
	}
	charge += int64(nullCount)
	for _, value := range texts {
		if int64(len(value)) > math.MaxInt64-9-charge {
			return 0, failure("state_corrupt")
		}
		charge += 9 + int64(len(value))
	}
	for _, value := range blobs {
		if int64(len(value)) > math.MaxInt64-9-charge {
			return 0, failure("state_corrupt")
		}
		charge += 9 + int64(len(value))
	}
	return charge, nil
}

func sqliteMetaCharge(meta sqliteStoreMeta) (int64, error) {
	revision, err := sqliteEncodeUint64(meta.Revision)
	if err != nil {
		return 0, err
	}
	// Included INTEGERs: singleton, schema_version, legacy_schema_version,
	// sync_enabled. The remaining six columns below are TEXT/BLOB or NULL.
	// Exclude only logical_bytes and durability_nonce, including type bytes.
	texts := []string{meta.ComputerID, meta.StateBasename, meta.DatabaseBasename}
	nulls := 0
	for _, value := range []*string{meta.MigrationID, meta.BackupSHA256} {
		if value == nil {
			nulls++
		} else {
			texts = append(texts, *value)
		}
	}
	return sqliteRowCharge(4, nulls, texts, [][]byte{revision[:]})
}
