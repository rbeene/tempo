//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

type caQACell struct {
	value sqliteio.Value
	expr  string
}

func caQARows() [][4]caQACell {
	rows := make([][4]caQACell, len(sqliteCaptureCatalog))
	for i, row := range sqliteCaptureCatalog {
		for j, value := range row {
			rows[i][j] = caQACell{sqliteio.Text(value), "?"}
		}
	}
	return rows
}

// A read-only CTE shadows sqlite_schema for this exact fixed query. Every
// tuple cell is a real bind; no schema file or producer API is rewritten.
func caQAQuery(t *testing.T, tx *sqliteio.Tx, rows [][4]caQACell) string {
	t.Helper()
	var sql strings.Builder
	sql.WriteString("WITH sqlite_schema(type,name,tbl_name,sql) AS (VALUES ")
	values := make([]sqliteio.Value, 0, len(rows)*4+1)
	for i, row := range rows {
		if i != 0 {
			sql.WriteByte(',')
		}
		sql.WriteByte('(')
		for j, cell := range row {
			if j != 0 {
				sql.WriteByte(',')
			}
			sql.WriteString(cell.expr)
			values = append(values, cell.value)
		}
		sql.WriteByte(')')
	}
	sql.WriteString(") ")
	sql.WriteString(sqliteCaptureCatalogSQL)
	values = append(values, sqliteio.Integer(int64(len(sqliteCaptureCatalog)+1)))
	s, err := tx.Prepare(sql.String(), values...)
	if err != nil {
		t.Fatal("native shadow catalog prepare", err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Error("native shadow catalog finalize", err)
		}
	}()
	row, err := s.Step()
	if err != nil || !row || s.ColumnCount() != 1 {
		t.Fatal("native shadow catalog ROW", err)
	}
	kind, err := s.Kind(0)
	if err != nil || kind != sqliteio.TextKind {
		t.Fatal("native shadow catalog TEXT", kind, err)
	}
	value, err := s.Text(0)
	if err != nil {
		t.Fatal("native shadow catalog value", err)
	}
	if row, err = s.Step(); err != nil || row {
		t.Fatal("native shadow catalog DONE", err)
	}
	return value
}

func caQAStateCorrupt(t *testing.T, err error) {
	t.Helper()
	var domain *Error
	if !errors.As(err, &domain) || domain.Code != "state_corrupt" {
		t.Fatalf("exact malformed catalog refusal: %v", err)
	}
}

func TestSQLiteCatalogAggregateNativeTypedShadow(t *testing.T) {
	f := interopLocation(t)
	interopInitialize(t, f, interopMeta(f))
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	// Prove the CTE really shadows sqlite_schema before using it as an oracle.
	control := [][4]caQACell{{{sqliteio.Text("table"), "?"}, {sqliteio.Text("qa_shadow_only"), "?"}, {sqliteio.Text("qa_shadow_only"), "?"}, {sqliteio.Text("CREATE TABLE qa_shadow_only(x TEXT)"), "?"}}}
	var one [][4]string
	if err := json.Unmarshal([]byte(caQAQuery(t, tx, control)), &one); err != nil || len(one) != 1 || one[0][1] != "qa_shadow_only" {
		t.Fatal("shadow CTE did not replace actual sqlite_schema", err)
	}
	canonical := caQAQuery(t, tx, caQARows())
	var got [][4]string
	if err := json.Unmarshal([]byte(canonical), &got); err != nil || !reflect.DeepEqual(got, sqliteCaptureCatalog[:]) {
		t.Fatal("native aggregate is not ordered nested 85-by-4 TEXT arrays", err)
	}
	if err := sqliteCheckCaptureCatalogJSON(canonical); err != nil {
		t.Fatal("native canonical aggregate rejected", err)
	}
	for column := 0; column < 4; column++ {
		for _, kind := range []string{"NULL", "INTEGER", "REAL", "BLOB"} {
			t.Run(kind+"/column-"+string(rune('0'+column)), func(t *testing.T) {
				rows := caQARows()
				switch kind {
				case "NULL":
					rows[0][column] = caQACell{sqliteio.Null(), "?"}
				case "INTEGER":
					rows[0][column] = caQACell{sqliteio.Integer(7), "?"}
				case "REAL":
					rows[0][column] = caQACell{sqliteio.Integer(7), "CAST(? AS REAL)"}
				case "BLOB":
					rows[0][column] = caQACell{sqliteio.Blob([]byte(`"index"`)), "?"}
				}
				raw := caQAQuery(t, tx, rows)
				var entries []json.RawMessage
				if err := json.Unmarshal([]byte(raw), &entries); err != nil {
					t.Fatal("typed aggregate JSON", err)
				}
				// A NULL name is excluded by WHERE name NOT GLOB before the
				// CASE type guard runs; the missing row must still be refused.
				wantRows, wantNulls := len(sqliteCaptureCatalog), 1
				if kind == "NULL" && column == 1 {
					wantRows, wantNulls = len(sqliteCaptureCatalog)-1, 0
				}
				nulls := 0
				for _, entry := range entries {
					if bytes.Equal(bytes.TrimSpace(entry), []byte("null")) {
						nulls++
					}
				}
				if len(entries) != wantRows || nulls != wantNulls {
					t.Fatalf("typed catalog row/refusal witness %s column %d: rows=%d nulls=%d", kind, column, len(entries), nulls)
				}
				caQAStateCorrupt(t, sqliteCheckCaptureCatalogJSON(raw))
			})
		}
	}
	interopRollback(t, tx)
	interopClose(t, c)
}

// The pinned native engine's JSONB string is BLOB evidence and cannot pass
// the aggregate's TEXT guard. An unexpected native error fails calibration.
func TestSQLiteCatalogAggregateJSONBStringCannotLaunderBlob(t *testing.T) {
	f := interopLocation(t)
	interopInitialize(t, f, interopMeta(f))
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	s, err := tx.Prepare("SELECT typeof(jsonb(?))", sqliteio.Text(`"index"`))
	if err != nil {
		t.Fatal("native JSONB positive control prepare", err)
	}
	row, stepErr := s.Step()
	if stepErr != nil || !row {
		closeErr := s.Close()
		t.Fatal("native JSONB string ROW positive control", stepErr, closeErr)
	}
	value, textErr := s.Text(0)
	done, doneErr := s.Step()
	closeErr := s.Close()
	if textErr != nil || value != "blob" || doneErr != nil || done || closeErr != nil {
		t.Fatal("native JSONB string positive control", stepErr, textErr, doneErr, closeErr)
	}
	rows := caQARows()
	rows[0][0] = caQACell{sqliteio.Text(`"index"`), "jsonb(?)"}
	raw := caQAQuery(t, tx, rows)
	var entries []json.RawMessage
	if err := json.Unmarshal([]byte(raw), &entries); err != nil || len(entries) != len(sqliteCaptureCatalog) {
		t.Fatal("native JSONB BLOB was laundered as catalog TEXT", err)
	}
	nulls := 0
	for _, entry := range entries {
		if bytes.Equal(bytes.TrimSpace(entry), []byte("null")) {
			nulls++
		}
	}
	if nulls != 1 {
		t.Fatal("native JSONB BLOB was laundered as catalog TEXT", nulls)
	}
	caQAStateCorrupt(t, sqliteCheckCaptureCatalogJSON(raw))
	interopRollback(t, tx)
	interopClose(t, c)
}

func TestSQLiteCatalogAggregateNativeFaultsJoinAndStopBeforeMeta(t *testing.T) {
	for _, stage := range []string{"ROW", "DONE", "finalize", "malformed+finalize"} {
		t.Run(stage, func(t *testing.T) {
			f := interopLocation(t)
			interopInitialize(t, f, interopMeta(f))
			if stage == "malformed+finalize" {
				c, tx := interopOpen(t, f, false, sqliteio.Write)
				interopDone(t, tx, "CREATE TABLE qa_extra_catalog(x TEXT) STRICT")
				interopCommit(t, tx)
				interopClose(t, c)
			}
			c, tx := interopOpen(t, f, false, sqliteio.Read)
			fired, prepares := false, 0
			spQAHooks(t, spQASQLHooks{
				Observe: func(e spQASQLEvent) {
					if e.Operation == "prepare" && e.Phase == "prepare-before" {
						prepares++
					}
				},
				Fault: func(e spQASQLEvent) error {
					if fired || e.Operation != "statement" {
						return nil
					}
					atRow := stage == "ROW" && e.Phase == "step-after" && e.Code == 100
					atDone := stage == "DONE" && e.Phase == "step-after" && e.Code == 101
					atClose := (stage == "finalize" || stage == "malformed+finalize") && e.Phase == "finalize-after" && e.Code == 0
					if atRow || atDone || atClose {
						fired = true
						return sqliteio.ErrUnsafe
					}
					return nil
				},
			})
			got, err := sqliteReadCaptureSchema(tx, f.authority, f.database)
			spQASetSQLHooks(spQASQLHooks{})
			if !fired || prepares != 1 || !reflect.DeepEqual(got, sqliteStoreMeta{}) || !errors.Is(err, sqliteio.ErrUnsafe) {
				t.Fatalf("catalog fault failed to stop before metadata: stage=%s fired=%t prepares=%d meta=%+v err=%v", stage, fired, prepares, got, err)
			}
			if stage == "malformed+finalize" {
				caQAStateCorrupt(t, err)
			}
			interopRollback(t, tx)
			interopClose(t, c)
		})
	}
}
