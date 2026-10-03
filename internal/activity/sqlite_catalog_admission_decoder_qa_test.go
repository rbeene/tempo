//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestSQLiteCatalogAdmissionAggregateRejectsNonTextAndMalformedShapes(t *testing.T) {
	copyRows := func() [][]any {
		rows := make([][]any, len(sqliteCaptureCatalog))
		for i, row := range sqliteCaptureCatalog {
			rows[i] = []any{row[0], row[1], row[2], row[3]}
		}
		return rows
	}
	encode := func(value any) string {
		b, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	if err := sqliteCheckCaptureCatalogJSON(encode(copyRows())); err != nil {
		t.Fatal("exact JSON control", err)
	}
	cases := []struct {
		name string
		raw  string
	}{
		{"top-level null", "null"},
		{"trailing token", encode(copyRows()) + " true"},
		{"invalid UTF-8", string([]byte{'[', 0xff, ']'})},
		{"quoted nested JSON", encode([]any{encode(copyRows()[0])})},
	}
	for column := 0; column < 4; column++ {
		rows := copyRows()
		rows[0][column] = nil
		cases = append(cases, struct{ name, raw string }{name: "null column " + string(rune('0'+column)), raw: encode(rows)})
		rows = copyRows()
		rows[0][column] = 1
		cases = append(cases, struct{ name, raw string }{name: "number column " + string(rune('0'+column)), raw: encode(rows)})
	}
	rows := copyRows()
	cases = append(cases, struct{ name, raw string }{name: "missing row", raw: encode(rows[:len(rows)-1])})
	rows = copyRows()
	cases = append(cases, struct{ name, raw string }{name: "extra row", raw: encode(append(rows, rows[0]))})
	rows = copyRows()
	rows[0], rows[1] = rows[1], rows[0]
	cases = append(cases, struct{ name, raw string }{name: "wrong order", raw: encode(rows)})
	rows = copyRows()
	rows[0][3] = rows[0][3].(string) + " "
	cases = append(cases, struct{ name, raw string }{name: "changed DDL byte", raw: encode(rows)})
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var domain *Error
			if err := sqliteCheckCaptureCatalogJSON(tc.raw); !errors.As(err, &domain) || domain.Code != "state_corrupt" {
				t.Fatalf("malformed aggregate accepted or wrong refusal: %v", err)
			}
		})
	}
}
