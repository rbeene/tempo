//go:build (darwin || linux) && (amd64 || arm64)

package activity

// Independent source-first QA for approved fresh-only contract b5a92cc0.
// Root owns formatting, compilation, native execution and implementation.
import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

func TestSQLiteFrontierPendingIntervalLocalFreshRoundTripChargeChildrenOwnership(t *testing.T) {
	source := fpiQALegacy(t)
	frow, prow, irow := fpiQARows(t, source)
	f, m, original := fpiQASeed(t, source)
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	var total int64
	for _, x := range []struct {
		name string
		run  func() (int64, error)
		want int64
	}{
		{"frontier-charge", func() (int64, error) { return sqliteFrontierLocalCharge(frow) }, fpiQAFCharge(t, frow)},
		{"pending-charge", func() (int64, error) { return sqlitePendingLocalCharge(prow) }, fpiQAPCharge(t, prow)},
		{"interval-charge", func() (int64, error) { return sqliteIntervalLocalCharge(irow) }, fpiQAIGCharge(t, irow)},
	} {
		n, err := x.run()
		if err != nil || n != x.want {
			t.Fatal(x.name, n, x.want, err)
		}
	}
	if next, err := sqliteNextIntervalOrdinalLocal(tx); err != nil || next != 0 {
		t.Fatal("fresh empty ordinal", err)
	}
	n, err := sqliteWriteFrontierLocal(tx, source.ComputerID, nil, frow)
	if err != nil || n != fpiQAFCharge(t, frow) {
		t.Fatal(err)
	}
	total += n
	n, err = sqliteWritePendingLocal(tx, source.ComputerID, nil, prow)
	if err != nil || n != fpiQAPCharge(t, prow) {
		t.Fatal(err)
	}
	total += n
	n, err = sqliteInsertIntervalLocal(tx, source.ComputerID, irow)
	if err != nil || n != fpiQAIGCharge(t, irow) {
		t.Fatal(err)
	}
	total += n
	beforeF, beforeP, beforeI := frow, prow, irow
	got, ok, err := sqliteReadFrontierLocal(tx, source.ComputerID, frow.ID)
	if err != nil || !ok {
		t.Fatal(err)
	}
	fpiQASame(t, got, asQAJSON(t, frow))
	got.Attribution.TaskID = "caller-mutation"
	if again, found, err := sqliteReadFrontierLocal(tx, source.ComputerID, frow.ID); err != nil || !found || again != asQAJSON(t, frow) {
		t.Fatal("returned scalar ownership", err)
	}
	gp, ok, err := sqliteReadPendingLocal(tx, source.ComputerID, prow.ID)
	if err != nil || !ok {
		t.Fatal(err)
	}
	fpiQASame(t, gp, asQAJSON(t, prow))
	gi, ok, err := sqliteReadIntervalLocal(tx, source.ComputerID, irow.ID)
	if err != nil || !ok {
		t.Fatal(err)
	}
	fpiQASame(t, gi, asQAJSON(t, irow))
	empty, err := sqliteComponentSegmentIDsLocal(tx, source.ComputerID, frow.ID)
	if err != nil || empty == nil || len(empty) != 0 {
		t.Fatal("allocated empty membership", err)
	}
	supports, err := sqliteIntervalSegmentIDsLocal(tx, source.ComputerID, irow.ID)
	if err != nil || supports == nil || len(supports) != 0 {
		t.Fatal("allocated empty supports", err)
	}
	if next, err := sqliteNextIntervalSegmentOrdinalLocal(tx, source.ComputerID, irow.ID); err != nil || next != 0 {
		t.Fatal(err)
	}
	ids := source.Intervals[0].SegmentIDs
	for ordinal, id := range ids {
		ch, err := sqliteComponentSegmentLocalCharge(frow.ID, id)
		if err != nil || ch != fpiQAMCharge(frow.ID, id) {
			t.Fatal(err)
		}
		n, err = sqliteInsertComponentSegmentLocal(tx, source.ComputerID, frow.ID, id)
		if err != nil || n != ch {
			t.Fatal(err)
		}
		total += n
		ch, err = sqliteIntervalSegmentLocalCharge(irow.ID, int64(ordinal), id)
		if err != nil || ch != fpiQASCharge(irow.ID, id) {
			t.Fatal(err)
		}
		n, err = sqliteAppendIntervalSegmentLocal(tx, source.ComputerID, irow.ID, int64(ordinal), id)
		if err != nil || n != ch {
			t.Fatal(err)
		}
		total += n
		owner, found, err := sqliteComponentForSegmentLocal(tx, source.ComputerID, id)
		if err != nil || !found || owner != frow.ID {
			t.Fatal(err)
		}
	}
	sorted := append([]string(nil), ids...)
	sort.Strings(sorted)
	list, err := sqliteComponentSegmentIDsLocal(tx, source.ComputerID, frow.ID)
	if err != nil {
		t.Fatal(err)
	}
	fpiQASame(t, list, sorted)
	list[0] = fpiQAMissing
	list, err = sqliteComponentSegmentIDsLocal(tx, source.ComputerID, frow.ID)
	if err != nil {
		t.Fatal(err)
	}
	fpiQASame(t, list, sorted)
	supports, err = sqliteIntervalSegmentIDsLocal(tx, source.ComputerID, irow.ID)
	if err != nil {
		t.Fatal(err)
	}
	fpiQASame(t, supports, ids)
	supports[0] = fpiQAMissing
	supports, err = sqliteIntervalSegmentIDsLocal(tx, source.ComputerID, irow.ID)
	if err != nil {
		t.Fatal(err)
	}
	fpiQASame(t, supports, ids)
	if next, err := sqliteNextIntervalOrdinalLocal(tx); err != nil || next != 1 {
		t.Fatal(err)
	}
	if next, err := sqliteNextIntervalSegmentOrdinalLocal(tx, source.ComputerID, irow.ID); err != nil || next != 3 {
		t.Fatal(err)
	}
	for _, id := range ids {
		n, err = sqliteDeleteComponentSegmentLocal(tx, source.ComputerID, frow.ID, id)
		if err != nil || n != -fpiQAMCharge(frow.ID, id) {
			t.Fatal("pair delete charge", err)
		}
		total += n
	}
	n, err = sqliteDeleteFrontierLocal(tx, source.ComputerID, frow)
	if err != nil || n != -fpiQAFCharge(t, frow) {
		t.Fatal(err)
	}
	total += n
	// Pending local shape is independent of its temporarily removed frontier.
	gp, ok, err = sqliteReadPendingLocal(tx, source.ComputerID, prow.ID)
	if err != nil || !ok || gp != asQAJSON(t, prow) {
		t.Fatal("pending imposed premature owner read", err)
	}
	n, err = sqliteDeletePendingLocal(tx, source.ComputerID, prow)
	if err != nil || n != -fpiQAPCharge(t, prow) {
		t.Fatal(err)
	}
	total += n
	fpiQASame(t, frow, beforeF)
	fpiQASame(t, prow, beforeP)
	fpiQASame(t, irow, beforeI)
	next, snap := fpiQACommit(t, tx, m, total)
	interopClose(t, c)
	fpiQAReopen(t, f, next, snap)
	if reflect.DeepEqual(original, snap) {
		t.Fatal("committed local round-trip was vacuous")
	}
	c, tx = interopOpen(t, f, false, sqliteio.Read)
	supports, err = sqliteIntervalSegmentIDsLocal(tx, source.ComputerID, irow.ID)
	if err != nil {
		t.Fatal(err)
	}
	fpiQASame(t, supports, ids)
	interopRollback(t, tx)
	interopClose(t, c)
}

func TestSQLiteFrontierPendingLocalEveryIndependentOldAxisCASDeleteRollback(t *testing.T) {
	source := fpiQALegacy(t)
	base, _, sibling := fpiQARows(t, source)
	f, m, snap := fpiQASeed(t, source)
	for _, family := range []string{"frontier", "pending"} {
		for _, axis := range fpiQAAxes() {
			t.Run(family+"/"+axis, func(t *testing.T) {
				c, tx := interopOpen(t, f, false, sqliteio.Write)
				selected := fpiQAAxis(t, base, axis)
				if reflect.DeepEqual(fpiQAFrontierValues(t, base), fpiQAFrontierValues(t, selected)) {
					t.Fatal("vacuous old axis")
				}
				after := selected
				after.End = after.End.Add(2 * time.Second)
				if family == "frontier" {
					asQABindFixture(t, tx, "union_frontier", fpiQAFrontierCols, fpiQAFrontierValues(t, selected))
					d, err := sqliteWriteFrontierLocal(tx, selected.ComputerID, &selected, after)
					if err != nil || d != fpiQAFCharge(t, after)-fpiQAFCharge(t, selected) {
						t.Fatal("positive exact 27-bind CAS", err)
					}
					got, ok, err := sqliteReadFrontierLocal(tx, after.ComputerID, after.ID)
					if err != nil || !ok {
						t.Fatal(err)
					}
					fpiQASame(t, got, asQAJSON(t, after))
					d, err = sqliteWriteFrontierLocal(tx, selected.ComputerID, &after, selected)
					if err != nil || d != fpiQAFCharge(t, selected)-fpiQAFCharge(t, after) {
						t.Fatal("checked exact CAS restoration", err)
					}
					restored, found, err := sqliteReadFrontierLocal(tx, selected.ComputerID, selected.ID)
					if err != nil || !found {
						t.Fatal(err)
					}
					fpiQASame(t, restored, asQAJSON(t, selected))
					fpiQAOldAxisWitness(t, "frontier", axis, fpiQAFrontierValues(t, base), fpiQAFrontierValues(t, restored))
				} else {
					p, pa := fpiQAPendingFrom(selected), fpiQAPendingFrom(after)
					asQABindFixture(t, tx, "pending_finalization", fpiQAPendingCols, fpiQAPendingValues(t, p))
					d, err := sqliteWritePendingLocal(tx, p.ComputerID, &p, pa)
					if err != nil || d != fpiQAPCharge(t, pa)-fpiQAPCharge(t, p) {
						t.Fatal("positive exact 15-bind CAS", err)
					}
					got, ok, err := sqliteReadPendingLocal(tx, pa.ComputerID, pa.ID)
					if err != nil || !ok {
						t.Fatal(err)
					}
					fpiQASame(t, got, asQAJSON(t, pa))
					d, err = sqliteWritePendingLocal(tx, p.ComputerID, &pa, p)
					if err != nil || d != fpiQAPCharge(t, p)-fpiQAPCharge(t, pa) {
						t.Fatal("checked exact CAS restoration", err)
					}
					restored, found, err := sqliteReadPendingLocal(tx, p.ComputerID, p.ID)
					if err != nil || !found {
						t.Fatal(err)
					}
					fpiQASame(t, restored, asQAJSON(t, p))
					fpiQAOldAxisWitness(t, "pending", axis, fpiQAPendingValues(t, fpiQAPendingFrom(base)), fpiQAPendingValues(t, restored))
				}
				after = selected // Exact delete below uses the actual restored row.
				// An earlier independently staged sibling and metadata change must roll back.
				d, err := sqliteInsertIntervalLocal(tx, source.ComputerID, sibling)
				if err != nil || d != fpiQAIGCharge(t, sibling) {
					t.Fatal(err)
				}
				next := metaQANext(t, m)
				next.Revision = bump(m.Revision)
				next.LogicalBytes += d
				if err := sqliteUpdateMeta(tx, m, next); err != nil {
					t.Fatal(err)
				}
				before, _ := fpiQAAudit(t, tx)
				proposal := base
				proposal.End = proposal.End.Add(time.Second)
				if family == "frontier" {
					d, err = sqliteWriteFrontierLocal(tx, base.ComputerID, &base, proposal)
					bgQACorrupt(t, err)
					if d != 0 {
						t.Fatal("stale CAS charged")
					}
					d, err = sqliteDeleteFrontierLocal(tx, base.ComputerID, base)
					bgQACorrupt(t, err)
					if d != 0 {
						t.Fatal("stale delete charged")
					}
				} else {
					old, p := fpiQAPendingFrom(base), fpiQAPendingFrom(proposal)
					d, err = sqliteWritePendingLocal(tx, base.ComputerID, &old, p)
					bgQACorrupt(t, err)
					if d != 0 {
						t.Fatal("stale CAS charged")
					}
					d, err = sqliteDeletePendingLocal(tx, base.ComputerID, old)
					bgQACorrupt(t, err)
					if d != 0 {
						t.Fatal("stale delete charged")
					}
				}
				got, _ := fpiQAAudit(t, tx)
				fpiQASame(t, got, before)
				if family == "frontier" {
					d, err = sqliteDeleteFrontierLocal(tx, after.ComputerID, after)
					if err != nil || d != -fpiQAFCharge(t, after) {
						t.Fatal("positive exact delete", err)
					}
				} else {
					p := fpiQAPendingFrom(after)
					d, err = sqliteDeletePendingLocal(tx, p.ComputerID, p)
					if err != nil || d != -fpiQAPCharge(t, p) {
						t.Fatal("positive exact delete", err)
					}
				}
				interopRollback(t, tx)
				interopClose(t, c)
				fpiQAReopen(t, f, m, snap)
			})
		}
	}
	for _, family := range []string{"frontier", "pending"} {
		for _, axis := range fpiQAAxes()[:7] {
			t.Run("immutable/"+family+"/"+axis, func(t *testing.T) {
				c, tx := interopOpen(t, f, false, sqliteio.Write)
				changed := fpiQAAxis(t, base, axis)
				after := base
				after.End = after.End.Add(time.Second)
				if family == "frontier" {
					d, err := sqliteWriteFrontierLocal(tx, base.ComputerID, nil, base)
					if err != nil || d <= 0 {
						t.Fatal(err)
					}
					if d, err = sqliteWriteFrontierLocal(tx, base.ComputerID, &base, after); err != nil {
						t.Fatal("ordinary update control", err)
					}
					before := after
					changed.Start, changed.End = after.Start, after.End
					d, err = sqliteWriteFrontierLocal(tx, base.ComputerID, &before, changed)
					rdQAPreSQLValidation(t, err)
					if d != 0 {
						t.Fatal("immutable update charged")
					}
				} else {
					p, pa := fpiQAPendingFrom(base), fpiQAPendingFrom(after)
					d, err := sqliteWritePendingLocal(tx, base.ComputerID, nil, p)
					if err != nil || d <= 0 {
						t.Fatal(err)
					}
					if _, err = sqliteWritePendingLocal(tx, base.ComputerID, &p, pa); err != nil {
						t.Fatal(err)
					}
					changed.Start, changed.End = after.Start, after.End
					bad := fpiQAPendingFrom(changed)
					d, err = sqliteWritePendingLocal(tx, base.ComputerID, &pa, bad)
					rdQAPreSQLValidation(t, err)
					if d != 0 {
						t.Fatal("immutable update charged")
					}
				}
				interopRollback(t, tx)
				interopClose(t, c)
				fpiQAReopen(t, f, m, snap)
			})
		}
	}
}

func fpiQARead(t *testing.T, tx *sqliteio.Tx, computer, family, id string) (any, bool, error) {
	switch family {
	case "frontier":
		return sqliteReadFrontierLocal(tx, computer, id)
	case "pending":
		return sqliteReadPendingLocal(tx, computer, id)
	case "interval":
		return sqliteReadIntervalLocal(tx, computer, id)
	}
	t.Fatal("unknown reader")
	return nil, false, nil
}
func fpiQAZero(family string) any {
	switch family {
	case "frontier":
		return sqliteFrontierLocalRow{}
	case "pending":
		return sqlitePendingLocalRow{}
	default:
		return sqliteIntervalLocalRow{}
	}
}
func TestSQLiteFrontierPendingIntervalLocalSelectedKindsAndCanonicalProjection(t *testing.T) {
	source := fpiQALegacy(t)
	fr, pr, ir := fpiQARows(t, source)
	f, m, snap := fpiQASeed(t, source)
	cases := []struct {
		family, table, cols, id string
		values                  []sqliteio.Value
	}{{"frontier", "union_frontier", fpiQAFrontierCols, fr.ID, fpiQAFrontierValues(t, fr)}, {"pending", "pending_finalization", fpiQAPendingCols, pr.ID, fpiQAPendingValues(t, pr)}, {"interval", "intervals", fpiQAIntervalCols, ir.ID, fpiQAIntervalValues(t, ir)}}
	for _, x := range cases {
		for i, col := range fpiQAColumns(x.cols) {
			if i == 0 {
				continue
			}
			for _, wrong := range []string{"NULL", "wrong-kind", "invalid-utf8"} {
				isText := (x.family != "pending" && (i <= 7 || i == 10 || i == 13)) || (x.family == "pending" && (i == 1 || i == 4 || i == 7))
				if wrong == "invalid-utf8" && !isText {
					continue
				}
				t.Run(x.family+"/"+col+"/"+wrong, func(t *testing.T) {
					c, tx := interopOpen(t, f, false, sqliteio.Write)
					fpiQAShadow(t, tx, x.table, x.cols)
					asQABindFixture(t, tx, x.table, x.cols, x.values)
					got, ok, err := fpiQARead(t, tx, source.ComputerID, x.family, x.id)
					if err != nil || !ok || reflect.DeepEqual(got, fpiQAZero(x.family)) {
						t.Fatal("positive selected projection control", err)
					}
					value := sqliteio.Null()
					if wrong == "wrong-kind" {
						value = sqliteio.Blob([]byte{1})
						if x.family == "interval" && col == "duration_ns" {
							value = sqliteio.Text("20000000000")
						}
					}
					if wrong == "invalid-utf8" {
						value = sqliteio.Text(string([]byte{0xff}))
					}
					interopDone(t, tx, "UPDATE "+x.table+" SET "+col+"=? WHERE "+fpiQAColumns(x.cols)[0]+"=?", value, sqliteio.Text(x.id))
					got, ok, err = fpiQARead(t, tx, source.ComputerID, x.family, x.id)
					bgQACorrupt(t, err)
					if ok || !reflect.DeepEqual(got, fpiQAZero(x.family)) {
						t.Fatal("corrupt read leaked row")
					}
					if x.family == "interval" && col == "creation_ordinal" {
						next, err := sqliteNextIntervalOrdinalLocal(tx)
						bgQACorrupt(t, err)
						if next != 0 {
							t.Fatal("bad selected global ordinal leaked next")
						}
					}
					interopRollback(t, tx)
					interopClose(t, c)
					fpiQAReopen(t, f, m, snap)
				})
			}
		}
	}
	for _, x := range cases {
		t.Run(x.family+"/foreign-selected-and-absence", func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			asQABindFixture(t, tx, x.table, x.cols, x.values)
			got, ok, err := fpiQARead(t, tx, asQAForeign, x.family, x.id)
			bgQACorrupt(t, err)
			if ok || !reflect.DeepEqual(got, fpiQAZero(x.family)) {
				t.Fatal("foreign row hidden or leaked")
			}
			missing := fpiQAID(900)
			if x.family == "interval" {
				missing = fpiQAMissing
			}
			got, ok, err = fpiQARead(t, tx, source.ComputerID, x.family, missing)
			if err != nil || ok || !reflect.DeepEqual(got, fpiQAZero(x.family)) {
				t.Fatal("absence control", err)
			}
			interopRollback(t, tx)
			interopClose(t, c)
		})
	}
	groups := []string{"", source.ComputerID + "/{}", strings.TrimSuffix(attributionKey(source.ComputerID, fr.Attribution), "}") + `,"unknown":"value"}`, attributionKey(source.ComputerID, fr.Attribution) + " ", source.ComputerID + `/{"account_id":"1","account_id":"1","user_id":"2","project_id":"3","task_id":"4","timezone":"UTC"}`, source.ComputerID + `/{"timezone":"UTC","task_id":"4","project_id":"3","user_id":"2","account_id":"1"}`, strings.Replace(attributionKey(source.ComputerID, fr.Attribution), "UTC", `\u0055TC`, 1), strings.Replace(attributionKey(source.ComputerID, fr.Attribution), "UTC", "bad\xff", 1)}
	for _, group := range groups {
		t.Run("pending-group-grammar", func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			asQABindFixture(t, tx, "pending_finalization", fpiQAPendingCols, fpiQAPendingValues(t, pr))
			if _, ok, err := sqliteReadPendingLocal(tx, source.ComputerID, pr.ID); err != nil || !ok {
				t.Fatal(err)
			}
			interopDone(t, tx, "UPDATE pending_finalization SET group_order=? WHERE component_id=?", sqliteio.Text(group), sqliteio.Text(pr.ID))
			got, ok, err := sqliteReadPendingLocal(tx, source.ComputerID, pr.ID)
			bgQACorrupt(t, err)
			if ok || got != (sqlitePendingLocalRow{}) {
				t.Fatal("noncanonical pending leaked")
			}
			interopRollback(t, tx)
			interopClose(t, c)
		})
	}
	for _, x := range cases {
		for _, defect := range []string{"nsec", "nsec-bound", "time-json", "group", "duration", "duration-width", "ordinal", "attribution", "reversed"} {
			if x.family != "interval" && (defect == "duration" || defect == "duration-width" || defect == "ordinal") {
				continue
			}
			if x.family == "pending" && defect == "attribution" {
				continue
			}
			t.Run(x.family+"/semantic/"+defect, func(t *testing.T) {
				c, tx := interopOpen(t, f, false, sqliteio.Write)
				asQABindFixture(t, tx, x.table, x.cols, x.values)
				col, value := "start_nsec", sqliteio.Integer(999999999)
				switch defect {
				case "time-json":
					col, value = "start_json", sqliteio.Text(`"2026-10-02T09:00:00.000000001Z"`)
				case "group":
					col, value = "group_order", sqliteio.Text(attributionKey(asQAForeign, fr.Attribution))
				case "duration":
					col, value = "duration_ns", interopCounter(t, "1")
				case "duration-width":
					col, value = "duration_ns", sqliteio.Blob([]byte{1})
				case "nsec-bound":
					col, value = "start_nsec", sqliteio.Integer(1000000000)
				case "reversed":
					col, value = "end_sec", sqliteio.Integer(fr.Start.Unix()-1)
				case "ordinal":
					col, value = "creation_ordinal", sqliteio.Integer(-1)
				case "attribution":
					col, value = "timezone", sqliteio.Text("not-a-zone")
				}
				if _, ok, err := fpiQARead(t, tx, source.ComputerID, x.family, x.id); err != nil || !ok {
					t.Fatal("semantic positive control", err)
				}
				if defect == "ordinal" || defect == "duration-width" || defect == "nsec-bound" || defect == "reversed" {
					fpiQAShadow(t, tx, x.table, x.cols)
				}
				interopDone(t, tx, "UPDATE "+x.table+" SET "+col+"=? WHERE "+fpiQAColumns(x.cols)[0]+"=?", value, sqliteio.Text(x.id))
				got, ok, err := fpiQARead(t, tx, source.ComputerID, x.family, x.id)
				bgQACorrupt(t, err)
				if ok || !reflect.DeepEqual(got, fpiQAZero(x.family)) {
					t.Fatal("semantic corruption leaked")
				}
				if defect == "ordinal" {
					next, err := sqliteNextIntervalOrdinalLocal(tx)
					bgQACorrupt(t, err)
					if next != 0 {
						t.Fatal("negative selected global ordinal leaked next")
					}
				}
				interopRollback(t, tx)
				interopClose(t, c)
			})
		}
	}
	fpiQAReopen(t, f, m, snap)
}

func TestSQLiteFrontierPendingIntervalLocalChildrenGapExhaustionUniquenessAndDeferredTarget(t *testing.T) {
	source := fpiQALegacy(t)
	fr, pr, ir := fpiQARows(t, source)
	f, m, snap := fpiQASeed(t, source)
	ids := source.Intervals[0].SegmentIDs
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	fpiQAFill(t, tx, fr, pr, ir)
	for i, id := range ids {
		if _, err := sqliteInsertComponentSegmentLocal(tx, source.ComputerID, fr.ID, id); err != nil {
			t.Fatal(err)
		}
		if _, err := sqliteAppendIntervalSegmentLocal(tx, source.ComputerID, ir.ID, int64(i), id); err != nil {
			t.Fatal(err)
		}
	}
	duplicate, err := sqliteInsertIntervalLocal(tx, ir.ComputerID, ir)
	fpiQANativeIdentity(t, err)
	if duplicate != 0 {
		t.Fatal("duplicate interval charged")
	}
	collision := ir
	collision.ID = fpiQAMissing
	duplicate, err = sqliteInsertIntervalLocal(tx, ir.ComputerID, collision)
	fpiQANativeIdentity(t, err)
	if duplicate != 0 {
		t.Fatal("duplicate creation ordinal charged")
	}
	d, err := sqliteInsertComponentSegmentLocal(tx, source.ComputerID, fr.ID, ids[0])
	fpiQANativeIdentity(t, err)
	if d != 0 {
		t.Fatal("duplicate member charged")
	}
	second := fr
	second.ID = fpiQAID(22)
	if _, err := sqliteWriteFrontierLocal(tx, source.ComputerID, nil, second); err != nil {
		t.Fatal(err)
	}
	d, err = sqliteInsertComponentSegmentLocal(tx, source.ComputerID, second.ID, ids[0])
	fpiQANativeIdentity(t, err)
	if d != 0 {
		t.Fatal("duplicate reverse ownership charged")
	}
	d, err = sqliteAppendIntervalSegmentLocal(tx, source.ComputerID, ir.ID, 3, ids[0])
	fpiQANativeIdentity(t, err)
	if d != 0 {
		t.Fatal("duplicate support ownership charged")
	}
	d, err = sqliteAppendIntervalSegmentLocal(tx, source.ComputerID, ir.ID, -1, fpiQAMissing)
	bgQAValidation(t, err)
	if d != 0 {
		t.Fatal("negative ordinal charged")
	}
	d, err = sqliteAppendIntervalSegmentLocal(tx, source.ComputerID, ir.ID, 2, fpiQAMissing)
	bgQACorrupt(t, err)
	if d != 0 {
		t.Fatal("stale position charged")
	}
	d, err = sqliteDeleteComponentSegmentLocal(tx, source.ComputerID, fr.ID, fpiQAMissing)
	bgQACorrupt(t, err)
	if d != 0 {
		t.Fatal("missing member deletion charged")
	}
	interopDone(t, tx, "DELETE FROM interval_segments WHERE interval_id=? AND ordinal=1", sqliteio.Text(ir.ID))
	list, err := sqliteIntervalSegmentIDsLocal(tx, source.ComputerID, ir.ID)
	bgQACorrupt(t, err)
	if list != nil {
		t.Fatal("gap leaked valid prefix")
	}
	if next, err := sqliteNextIntervalSegmentOrdinalLocal(tx, source.ComputerID, ir.ID); err != nil || next != 3 {
		t.Fatal("last-only next audited earlier gap", err)
	}
	interopDone(t, tx, "UPDATE interval_segments SET ordinal=? WHERE interval_id=? AND ordinal=2", sqliteio.Integer(math.MaxInt64), sqliteio.Text(ir.ID))
	if next, err := sqliteNextIntervalSegmentOrdinalLocal(tx, source.ComputerID, ir.ID); next != 0 {
		t.Fatal("exhaustion leaked next")
	} else {
		bgQAValidation(t, err)
	}
	interopDone(t, tx, "UPDATE intervals SET creation_ordinal=? WHERE interval_id=?", sqliteio.Integer(math.MaxInt64), sqliteio.Text(ir.ID))
	if next, err := sqliteNextIntervalOrdinalLocal(tx); next != 0 {
		t.Fatal("global exhaustion leaked next")
	} else {
		bgQAValidation(t, err)
	}
	interopRollback(t, tx)
	interopClose(t, c)
	fpiQAReopen(t, f, m, snap)
	for _, supply := range []bool{false, true} {
		t.Run("deferred-target", func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			fpiQAFill(t, tx, fr, pr, ir)
			d, err := sqliteInsertComponentSegmentLocal(tx, source.ComputerID, fr.ID, fpiQAMissing)
			if err != nil || d != fpiQAMCharge(fr.ID, fpiQAMissing) {
				t.Fatal("premature segment lookup", err)
			}
			d, err = sqliteAppendIntervalSegmentLocal(tx, source.ComputerID, ir.ID, 0, fpiQAMissing)
			if err != nil || d != fpiQASCharge(ir.ID, fpiQAMissing) {
				t.Fatal("premature support target lookup", err)
			}
			if err := tx.CheckForeignKeys(); err == nil {
				t.Fatal("actual missing-target FK control passed")
			}
			if supply {
				seg := asQASegment(source.Segments[ids[0]])
				seg.ID = fpiQAMissing
				if _, err := sqliteWriteSegmentLocal(tx, source.ComputerID, nil, seg); err != nil {
					t.Fatal("integrated target staging", err)
				}
				if err := tx.CheckForeignKeys(); err != nil {
					t.Fatal("supplied actual dependencies did not satisfy FKs", err)
				}
				interopRollback(t, tx)
			} else {
				outcome, err := tx.Commit()
				var native *sqliteio.Error
				if outcome != sqliteio.Unknown || !errors.As(err, &native) || native.Code&255 != 19 {
					t.Fatal("deferred COMMIT evidence", err)
				}
				if cleanup := tx.Rollback(); cleanup != nil {
					var n *sqliteio.Error
					if !errors.As(cleanup, &n) || n.Code&255 != 19 {
						t.Fatal("deferred cleanup evidence", cleanup)
					}
				}
			}
			interopClose(t, c)
			fpiQAReopen(t, f, m, snap)
		})
	}
	for _, family := range []string{"component", "support"} {
		t.Run("late-child-corrupt/"+family, func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			fpiQAFill(t, tx, fr, pr, ir)
			for i, id := range ids {
				if family == "component" {
					if _, err := sqliteInsertComponentSegmentLocal(tx, source.ComputerID, fr.ID, id); err != nil {
						t.Fatal(err)
					}
				} else {
					if _, err := sqliteAppendIntervalSegmentLocal(tx, source.ComputerID, ir.ID, int64(i), id); err != nil {
						t.Fatal(err)
					}
				}
			}
			var got []string
			var err error
			if family == "component" {
				got, err = sqliteComponentSegmentIDsLocal(tx, source.ComputerID, fr.ID)
			} else {
				got, err = sqliteIntervalSegmentIDsLocal(tx, source.ComputerID, ir.ID)
			}
			if err != nil || len(got) != 3 {
				t.Fatal("nonempty valid-prefix control", err)
			}
			if family == "component" {
				fpiQAShadow(t, tx, "component_segments", "component_id,segment_id")
				interopDone(t, tx, "UPDATE component_segments SET segment_id=? WHERE component_id=? AND segment_id=?", sqliteio.Text("zz-invalid-uuid"), sqliteio.Text(fr.ID), sqliteio.Text(got[2]))
				got, err = sqliteComponentSegmentIDsLocal(tx, source.ComputerID, fr.ID)
			} else {
				fpiQAShadow(t, tx, "interval_segments", "interval_id,ordinal,segment_id")
				interopDone(t, tx, "UPDATE interval_segments SET segment_id=? WHERE interval_id=? AND ordinal=2", sqliteio.Blob([]byte("wrong kind")), sqliteio.Text(ir.ID))
				got, err = sqliteIntervalSegmentIDsLocal(tx, source.ComputerID, ir.ID)
			}
			bgQACorrupt(t, err)
			if got != nil {
				t.Fatal("late corrupt child leaked accumulated prefix")
			}
			interopRollback(t, tx)
			interopClose(t, c)
			fpiQAReopen(t, f, m, snap)
		})
	}
}

func TestSQLiteFrontierPendingIntervalLocalTimePersistenceRefusalAndSaturation(t *testing.T) {
	source := fpiQALegacy(t)
	fr, pr, ir := fpiQARows(t, source)
	f, m, snap := fpiQASeed(t, source)
	// Raw-valid complete billing graph really becomes unreadable through legacy
	// JSON when one same-instant endpoint uses an unrepresentable -30s offset.
	raw := asQAJSON(t, source)
	raw.Intervals[0].End = raw.Intervals[0].End.In(time.FixedZone("minus-thirty", -30))
	item := raw.Outbox[raw.Intervals[0].ID]
	item.Interval = raw.Intervals[0]
	raw.Outbox[item.Interval.ID] = item
	h := qaNew(t)
	h.seed()
	bgQALegacyMarshalOracle(t, h.service, h.path, raw, false)
	base := time.Date(2026, 1, 1, 0, 0, 0, 123, time.UTC)
	// Use actual JSON twice: a first successful encode is insufficient when
	// minute truncation produces +/-00:00 which subsequently re-encodes as Z.
	jsonTime := func(t *testing.T, value time.Time) (time.Time, bool) {
		t.Helper()
		first, err := value.MarshalJSON()
		if err != nil {
			t.Fatal("actual time JSON first encoding", err)
		}
		var saved time.Time
		if err := json.Unmarshal(first, &saved); err != nil {
			t.Fatal("actual time JSON decoding", err)
		}
		second, err := saved.MarshalJSON()
		if err != nil {
			t.Fatal("actual time JSON repeated encoding", err)
		}
		return saved, string(first) == string(second)
	}
	for _, family := range []string{"frontier", "pending", "interval"} {
		for _, defect := range []string{"collapse", "reverse", "year", "duration-drift", "canonical-plus-thirty", "canonical-minus-thirty"} {
			if defect == "duration-drift" && family != "interval" {
				continue
			}
			t.Run(family+"/"+defect, func(t *testing.T) {
				c, tx := interopOpen(t, f, false, sqliteio.Write)
				r := fr
				r.Start = base
				r.End = base.Add(30 * time.Second).In(time.FixedZone("minus-ninety", -90))
				switch defect {
				case "reverse":
					r.End = base.Add(time.Second).In(time.FixedZone("minus-ninety", -90))
				case "year":
					r.Start = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
					r.End = r.Start.Add(time.Second)
					if _, err := r.Start.MarshalJSON(); err == nil {
						t.Fatal("year control encodable")
					}
				case "duration-drift":
					r.End = base.Add(time.Minute).In(time.FixedZone("plus-ninety", 90))
				case "canonical-plus-thirty", "canonical-minus-thirty":
					offset := 30
					if defect == "canonical-minus-thirty" {
						offset = -30
					}
					zone := time.FixedZone("canonical-byte-refusal", offset)
					r.Start, r.End = base.In(zone), base.Add(time.Second).In(zone)
				}
				if !r.End.After(r.Start) {
					t.Fatal("raw positive range premise false")
				}
				if defect != "year" {
					savedStart, startStable := jsonTime(t, r.Start)
					savedEnd, endStable := jsonTime(t, r.End)
					canonicalRefusal := defect == "canonical-plus-thirty" || defect == "canonical-minus-thirty"
					if canonicalRefusal {
						if startStable || endStable || !savedEnd.After(savedStart) || savedEnd.Sub(savedStart) != r.End.Sub(r.Start) {
							t.Fatal("canonical-byte refusal masked by final range/duration policy")
						}
					} else {
						if !startStable || !endStable {
							t.Fatal("final-policy witness masked by time JSON canonicalization refusal")
						}
						switch defect {
						case "collapse":
							if !savedEnd.Equal(savedStart) {
								t.Fatal("decoded collapse witness absent")
							}
						case "reverse":
							if !savedEnd.Before(savedStart) {
								t.Fatal("decoded reversed range witness absent")
							}
						case "duration-drift":
							if !savedEnd.After(savedStart) || savedEnd.Sub(savedStart) != r.End.Sub(r.Start)+30*time.Second {
								t.Fatal("decoded duration-only drift witness absent")
							}
						}
					}
				}
				original := r
				before, _ := fpiQAAudit(t, tx)
				var d int64
				var err error
				switch family {
				case "frontier":
					d, err = sqliteFrontierLocalCharge(r)
					rdQAPreSQLValidation(t, err)
					if d != 0 {
						t.Fatal("invalid final charge")
					}
					d, err = sqliteWriteFrontierLocal(tx, r.ComputerID, nil, r)
				case "pending":
					p := fpiQAPendingFrom(r)
					d, err = sqlitePendingLocalCharge(p)
					rdQAPreSQLValidation(t, err)
					if d != 0 {
						t.Fatal("invalid final charge")
					}
					d, err = sqliteWritePendingLocal(tx, p.ComputerID, nil, p)
				case "interval":
					in := ir
					in.Start, in.End = r.Start, r.End
					in.DurationNS = durationString(in.End.Sub(in.Start))
					d, err = sqliteIntervalLocalCharge(in)
					rdQAPreSQLValidation(t, err)
					if d != 0 {
						t.Fatal("invalid final duration charge")
					}
					d, err = sqliteInsertIntervalLocal(tx, in.ComputerID, in)
				}
				rdQAPreSQLValidation(t, err)
				if d != 0 {
					t.Fatal("pre-SQL validation charged")
				}
				after, _ := fpiQAAudit(t, tx)
				fpiQASame(t, after, before)
				fpiQASame(t, r, original)
				interopRollback(t, tx)
				interopClose(t, c)
				fpiQAReopen(t, f, m, snap)
			})
		}
	}
	for _, control := range []string{"offset-minute", "offset-subminute-both", "pre-epoch", "zero-wall-positive", "saturated-sub"} {
		t.Run(control, func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			start, end := base, base.Add(time.Second)
			switch control {
			case "offset-minute":
				start = start.In(time.FixedZone("named-minute", 3600))
				end = end.In(time.FixedZone("named-minute", 3600))
			case "offset-subminute-both":
				start = start.In(time.FixedZone("named-ninety", 90))
				end = end.In(time.FixedZone("named-ninety", 90))
			case "pre-epoch":
				start = time.Unix(-2, 123).UTC()
				end = time.Unix(-1, 123).UTC()
			case "zero-wall-positive":
				start = time.Time{}
				end = start.Add(time.Second)
			case "saturated-sub":
				start = time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
				end = time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)
			}
			savedStart, startStable := jsonTime(t, start)
			savedEnd, endStable := jsonTime(t, end)
			if !startStable || !endStable || !savedEnd.After(savedStart) || savedEnd.Sub(savedStart) != end.Sub(start) {
				t.Fatal("positive repeated JSON byte/range/duration premise false")
			}
			if control == "offset-subminute-both" && (!savedStart.Equal(start.Add(30*time.Second)) || !savedEnd.Equal(end.Add(30*time.Second))) {
				t.Fatal("equal subminute residual shifts absent")
			}
			r := fr
			r.Start, r.End = start, end
			p := fpiQAPendingFrom(r)
			in := ir
			in.Start, in.End = start, end
			in.DurationNS = durationString(end.Sub(start))
			if control == "saturated-sub" && in.DurationNS != "9223372036854775807" {
				t.Fatal("actual Sub saturation premise")
			}
			originalF, originalP, originalI := r, p, in
			var delta int64
			d, err := sqliteWriteFrontierLocal(tx, r.ComputerID, nil, r)
			if err != nil || d != fpiQAFCharge(t, r) {
				t.Fatal("ordinary frontier control", err)
			}
			delta += d
			d, err = sqliteWritePendingLocal(tx, p.ComputerID, nil, p)
			if err != nil || d != fpiQAPCharge(t, p) {
				t.Fatal("ordinary pending control", err)
			}
			delta += d
			d, err = sqliteInsertIntervalLocal(tx, in.ComputerID, in)
			if err != nil || d != fpiQAIGCharge(t, in) {
				t.Fatal("ordinary interval control", err)
			}
			delta += d
			got, ok, err := sqliteReadIntervalLocal(tx, in.ComputerID, in.ID)
			if err != nil || !ok {
				t.Fatal(err)
			}
			fpiQASame(t, got, asQAJSON(t, in))
			fpiQASame(t, r, originalF)
			fpiQASame(t, p, originalP)
			fpiQASame(t, in, originalI)
			if control == "offset-minute" {
				asQATimeWitness(t, start, got.Start)
				asQATimeWitness(t, end, got.End)
			}
			if control == "offset-subminute-both" && got.Start.Unix() == start.Unix() {
				t.Fatal("subminute persistence witness vacuous")
			}
			_, stored := fpiQAAudit(t, tx)
			if stored != m.LogicalBytes+delta {
				t.Fatal("literal positive encoded charge mismatch")
			}
			interopRollback(t, tx)
			interopClose(t, c)
			fpiQAReopen(t, f, m, snap)
		})
	}
	_ = pr
}

func TestSQLiteFrontierPendingIntervalLocalRawQueryInstantsNeighborsReservationsAndPendingCursor(t *testing.T) {
	source := fpiQALegacy(t)
	fr, _, ir := fpiQARows(t, source)
	f, m, snap := fpiQASeed(t, source)
	raw := time.Date(2026, 1, 1, 0, 0, 0, 123, time.FixedZone("subminute-query", 30))
	base := raw.UTC()
	saved := fpiQATimeOracle(t, raw)
	if raw.Unix() == saved.Unix() || raw.Nanosecond() != saved.Nanosecond() {
		t.Fatal("raw-query persistence distinction absent")
	}
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	fronts := []sqliteFrontierLocalRow{}
	intervals := []sqliteIntervalLocalRow{}
	for n, bounds := range [][2]int{{-10, -5}, {15, 20}, {40, 50}, {5, 10}} {
		r := fr
		r.ID = fpiQAID(100 + n)
		r.Start = base.Add(time.Duration(bounds[0]) * time.Second)
		r.End = base.Add(time.Duration(bounds[1]) * time.Second)
		if n == 3 {
			r.Attribution.TaskID = "5"
		}
		fronts = append(fronts, r)
		if _, err := sqliteWriteFrontierLocal(tx, r.ComputerID, nil, r); err != nil {
			t.Fatal(err)
		}
		in := ir
		in.ID = []string{"10000000-0000-4000-8000-000000000001", "10000000-0000-4000-8000-000000000002", "10000000-0000-4000-8000-000000000003", "10000000-0000-4000-8000-000000000004"}[n]
		in.Start, in.End, in.Attribution, in.Ordinal = r.Start, r.End, r.Attribution, int64(n)
		in.DurationNS = durationString(in.End.Sub(in.Start))
		intervals = append(intervals, in)
		if _, err := sqliteInsertIntervalLocal(tx, in.ComputerID, in); err != nil {
			t.Fatal(err)
		}
	}
	timer := sqliteTimerKey{ComputerID: source.ComputerID, AccountID: fr.Attribution.AccountID, ProjectID: fr.Attribution.ProjectID}
	for _, point := range []time.Time{raw, base} {
		got, ok, err := sqliteFrontierPredecessorLocal(tx, source.ComputerID, fr.Attribution, point)
		if err != nil || !ok || got != asQAJSON(t, fronts[0]) {
			t.Fatal("raw predecessor followed repaired point", err)
		}
		got, ok, err = sqliteFrontierSuccessorLocal(tx, source.ComputerID, fr.Attribution, point)
		if err != nil || !ok || got != asQAJSON(t, fronts[1]) {
			t.Fatal("full-attribution successor", err)
		}
		in, ok, err := sqliteIntervalPredecessorLocal(tx, timer, point)
		if err != nil || !ok || in != asQAJSON(t, intervals[0]) {
			t.Fatal("raw interval predecessor", err)
		}
		in, ok, err = sqliteIntervalSuccessorLocal(tx, timer, point)
		if err != nil || !ok || in != asQAJSON(t, intervals[3]) {
			t.Fatal("timer successor dropped other task", err)
		}
	}
	if got, ok, err := sqliteLatestIntervalLocal(tx, timer); err != nil || !ok || got != asQAJSON(t, intervals[2]) {
		t.Fatal("latest immutable end", err)
	}
	for _, point := range []time.Time{fronts[1].Start.Add(-time.Nanosecond), fronts[1].Start, fronts[1].Start.Add(time.Nanosecond)} {
		want := fronts[1]
		if point.Before(fronts[1].Start) {
			want = fronts[0]
		}
		got, ok, err := sqliteFrontierPredecessorLocal(tx, source.ComputerID, fr.Attribution, point)
		if err != nil || !ok || got != asQAJSON(t, want) {
			t.Fatal("nanosecond predecessor inclusive boundary", err)
		}
	}
	if got, ok, err := sqliteFrontierSuccessorLocal(tx, source.ComputerID, fr.Attribution, fronts[1].Start); err != nil || !ok || got != asQAJSON(t, fronts[2]) {
		t.Fatal("strict successor/multiple neighbors", err)
	}
	if got, ok, err := sqliteFrontierPredecessorLocal(tx, source.ComputerID, fr.Attribution, base.Add(-time.Hour)); err != nil || ok || got != (sqliteFrontierLocalRow{}) {
		t.Fatal("predecessor absence", err)
	}
	if got, ok, err := sqliteIntervalSuccessorLocal(tx, timer, base.Add(time.Hour)); err != nil || ok || got != (sqliteIntervalLocalRow{}) {
		t.Fatal("successor absence", err)
	}
	lower, upper := raw.Add(10*time.Second), raw.Add(15*time.Second)
	bounded := sqliteReservationWindow{Start: lower, End: &upper}
	original := bounded
	gotRows, err := sqliteFrontierReservationRowsLocal(tx, timer, bounded)
	if err != nil || gotRows == nil {
		t.Fatal(err)
	}
	fpiQASame(t, fpiQAIDs(gotRows), []string{fronts[3].ID, fronts[1].ID})
	fpiQASame(t, bounded, original)
	gotRows[0].Attribution.TaskID = "caller-owned"
	gotRows, err = sqliteFrontierReservationRowsLocal(tx, timer, bounded)
	if err != nil || gotRows[0] != asQAJSON(t, fronts[3]) {
		t.Fatal("reservation ownership", err)
	}
	gotRows, err = sqliteFrontierReservationRowsLocal(tx, timer, sqliteReservationWindow{Start: raw})
	if err != nil {
		t.Fatal(err)
	}
	fpiQASame(t, fpiQAIDs(gotRows), []string{fronts[3].ID, fronts[1].ID, fronts[2].ID})
	// old/new union reaches disconnected timer groups without a single-neighbor shortcut.
	oldUpper := raw.Add(-5 * time.Second)
	oldRows, err := sqliteFrontierReservationRowsLocal(tx, timer, sqliteReservationWindow{Start: raw.Add(-6 * time.Second), End: &oldUpper})
	if err != nil {
		t.Fatal(err)
	}
	fpiQASame(t, fpiQAIDs(oldRows), []string{fronts[0].ID})
	same := upper
	gotRows, err = sqliteFrontierReservationRowsLocal(tx, timer, sqliteReservationWindow{Start: same, End: &same})
	if err != nil || len(gotRows) != 1 || gotRows[0].ID != fronts[1].ID {
		t.Fatal("zero-width inclusive reservation contact", err)
	}
	badEnd := lower.Add(-time.Nanosecond)
	gotRows, err = sqliteFrontierReservationRowsLocal(tx, timer, sqliteReservationWindow{Start: lower, End: &badEnd})
	bgQAValidation(t, err)
	if gotRows != nil {
		t.Fatal("invalid window leaked rows")
	}
	// Keyset tokens are materialized canonical rows. Previous owner may be removed;
	// retained blocked rows likewise must not be returned again after that token.
	pending := []sqlitePendingLocalRow{}
	for _, r := range fronts[:3] {
		p := fpiQAPendingFrom(r)
		if _, err := sqliteWritePendingLocal(tx, source.ComputerID, nil, p); err != nil {
			t.Fatal(err)
		}
		pending = append(pending, asQAJSON(t, p))
	}
	var cursor *sqlitePendingLocalRow
	for n, want := range pending {
		row, ok, err := sqliteNextPendingLocal(tx, source.ComputerID, cursor)
		if err != nil || !ok || row != want {
			t.Fatal("ordered keyset row", n, err)
		}
		savedCursor := row
		cursor = &savedCursor
		if n != 1 {
			if _, err := sqliteDeletePendingLocal(tx, source.ComputerID, row); err != nil {
				t.Fatal("removed-token continuation", err)
			}
		}
	}
	if row, ok, err := sqliteNextPendingLocal(tx, source.ComputerID, cursor); err != nil || ok || row != (sqlitePendingLocalRow{}) {
		t.Fatal("keyset exhaustion", err)
	}
	if row, ok, err := sqliteNextPendingLocal(tx, source.ComputerID, nil); err != nil || !ok || row != pending[1] {
		t.Fatal("retained blocked pending row vanished", err)
	}
	asQAPlan(t, tx, "SELECT "+fpiQAFrontierCols+" FROM union_frontier WHERE computer_id=? AND account_id=? AND user_id=? AND project_id=? AND task_id=? AND timezone=? AND (start_sec,start_nsec)<=(?,?) ORDER BY start_sec DESC,start_nsec DESC,end_sec DESC,end_nsec DESC LIMIT 1", "frontier_group_start", sqliteio.Text(source.ComputerID), sqliteio.Text(fr.Attribution.AccountID), sqliteio.Text(fr.Attribution.UserID), sqliteio.Text(fr.Attribution.ProjectID), sqliteio.Text(fr.Attribution.TaskID), sqliteio.Text(fr.Attribution.Timezone), sqliteio.Integer(raw.Unix()), sqliteio.Integer(int64(raw.Nanosecond())))
	asQAPlan(t, tx, "SELECT "+fpiQAIntervalCols+" FROM intervals WHERE computer_id=? AND account_id=? AND project_id=? AND (start_sec,start_nsec)>(?,?) ORDER BY start_sec ASC,start_nsec ASC,end_sec ASC,end_nsec ASC LIMIT 1", "interval_timer_start", sqliteio.Text(source.ComputerID), sqliteio.Text(fr.Attribution.AccountID), sqliteio.Text(fr.Attribution.ProjectID), sqliteio.Integer(raw.Unix()), sqliteio.Integer(int64(raw.Nanosecond())))
	fpiQAOrderedPlan(t, tx, "SELECT "+fpiQAPendingCols+" FROM pending_finalization ORDER BY group_order,start_sec,start_nsec,end_sec,end_nsec,component_id LIMIT 1", "pending_order")
	interopRollback(t, tx)
	interopClose(t, c)
	fpiQAReopen(t, f, m, snap)
}

func TestSQLiteFrontierPendingIntervalLocalSelectedNeighborAndLateReservationCorruption(t *testing.T) {
	source := fpiQALegacy(t)
	fr, pr, ir := fpiQARows(t, source)
	f, m, snap := fpiQASeed(t, source)
	timer := sqliteTimerKey{ComputerID: source.ComputerID, AccountID: fr.Attribution.AccountID, ProjectID: fr.Attribution.ProjectID}
	for _, which := range []string{"frontier-pre", "frontier-post", "interval-pre", "interval-post", "latest", "pending-first", "pending-next", "reservation-late"} {
		t.Run(which, func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			fpiQAFill(t, tx, fr, pr, ir)
			point := fr.Start.Add(time.Second)
			check := func(wantError bool) {
				var err error
				var found bool
				var zero bool
				switch which {
				case "frontier-pre":
					var row sqliteFrontierLocalRow
					row, found, err = sqliteFrontierPredecessorLocal(tx, fr.ComputerID, fr.Attribution, point)
					zero = row == (sqliteFrontierLocalRow{})
				case "frontier-post":
					var row sqliteFrontierLocalRow
					row, found, err = sqliteFrontierSuccessorLocal(tx, fr.ComputerID, fr.Attribution, fr.Start.Add(-time.Second))
					zero = row == (sqliteFrontierLocalRow{})
				case "interval-pre":
					var row sqliteIntervalLocalRow
					row, found, err = sqliteIntervalPredecessorLocal(tx, timer, point)
					zero = row == (sqliteIntervalLocalRow{})
				case "interval-post":
					var row sqliteIntervalLocalRow
					row, found, err = sqliteIntervalSuccessorLocal(tx, timer, ir.Start.Add(-time.Second))
					zero = row == (sqliteIntervalLocalRow{})
				case "latest":
					var row sqliteIntervalLocalRow
					row, found, err = sqliteLatestIntervalLocal(tx, timer)
					zero = row == (sqliteIntervalLocalRow{})
				case "pending-first":
					var row sqlitePendingLocalRow
					row, found, err = sqliteNextPendingLocal(tx, fr.ComputerID, nil)
					zero = row == (sqlitePendingLocalRow{})
				case "pending-next":
					cursor := pr
					cursor.ID = fpiQAID(800)
					cursor.Start = cursor.Start.Add(-time.Second)
					var row sqlitePendingLocalRow
					row, found, err = sqliteNextPendingLocal(tx, fr.ComputerID, &cursor)
					zero = row == (sqlitePendingLocalRow{})
				case "reservation-late":
					var rows []sqliteFrontierLocalRow
					rows, err = sqliteFrontierReservationRowsLocal(tx, timer, sqliteReservationWindow{Start: fr.Start})
					found = len(rows) > 0
					zero = rows == nil
				}
				if wantError {
					bgQACorrupt(t, err)
					if found || !zero {
						t.Fatal("selected failure leaked row/list")
					}
				} else {
					if err != nil || !found || zero {
						t.Fatal("valid selected control", err)
					}
				}
			}
			if which == "reservation-late" {
				second := fr
				second.ID = fpiQAID(300)
				second.Start = fr.End.Add(time.Second)
				second.End = second.Start.Add(time.Second)
				if _, err := sqliteWriteFrontierLocal(tx, fr.ComputerID, nil, second); err != nil {
					t.Fatal(err)
				}
				check(false)
				interopDone(t, tx, "UPDATE union_frontier SET end_json=? WHERE component_id=?", sqliteio.Text(`"2000-01-01T00:00:00Z"`), sqliteio.Text(second.ID))
			} else {
				check(false)
				table, key, id := "union_frontier", "component_id", fr.ID
				if strings.HasPrefix(which, "interval") || which == "latest" {
					table, key, id = "intervals", "interval_id", ir.ID
				}
				if strings.HasPrefix(which, "pending") {
					table, key, id = "pending_finalization", "component_id", pr.ID
				}
				interopDone(t, tx, "UPDATE "+table+" SET end_json=? WHERE "+key+"=?", sqliteio.Text(`"2000-01-01T00:00:00Z"`), sqliteio.Text(id))
			}
			check(true)
			interopRollback(t, tx)
			interopClose(t, c)
			fpiQAReopen(t, f, m, snap)
		})
	}
}

func TestSQLiteFrontierPendingIntervalLocalCancellationEveryDatabaseAPIAndNativeFailureRollback(t *testing.T) {
	source := fpiQALegacy(t)
	fr, pr, ir := fpiQARows(t, source)
	f, m, snap := fpiQASeed(t, source)
	id := source.Intervals[0].SegmentIDs[0]
	timer := sqliteTimerKey{ComputerID: source.ComputerID, AccountID: fr.Attribution.AccountID, ProjectID: fr.Attribution.ProjectID}
	operations := []string{"read-frontier", "write-frontier", "delete-frontier", "component-for", "component-list", "component-insert", "component-delete", "read-pending", "write-pending", "delete-pending", "next-pending", "read-interval", "insert-interval", "next-interval-ordinal", "support-list", "support-next", "support-append", "frontier-pre", "frontier-post", "reservation", "interval-pre", "interval-post", "latest"}
	for _, operation := range operations {
		t.Run(operation, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			defer cancel()
			c, err := sqliteio.Open(ctx, f.directory, f.database, sqliteio.Options{Create: false, AcquireDeadline: time.Now().Add(250 * time.Millisecond)})
			if c != nil {
				t.Cleanup(func() {
					if cleanup := c.Close(context.Background()); cleanup != nil {
						interopSafeError(t, cleanup, f.directory)
					}
				})
			}
			if err != nil {
				t.Fatal("fresh cancellable connection", err)
			}
			tx, err := c.Begin(ctx, sqliteio.Write)
			if tx != nil {
				t.Cleanup(func() {
					if cleanup := tx.Rollback(); cleanup != nil {
						interopSafeError(t, cleanup, f.directory)
					}
				})
			}
			if err != nil {
				t.Fatal("single owned Begin", err)
			}
			fpiQAFill(t, tx, fr, pr, ir)
			if _, err := sqliteInsertComponentSegmentLocal(tx, fr.ComputerID, fr.ID, id); err != nil {
				t.Fatal(err)
			}
			next := metaQANext(t, m)
			next.Revision = bump(m.Revision)
			next.LogicalBytes += fpiQAFCharge(t, fr) + fpiQAPCharge(t, pr) + fpiQAIGCharge(t, ir) + fpiQAMCharge(fr.ID, id)
			if err := sqliteUpdateMeta(tx, m, next); err != nil {
				t.Fatal(err)
			}
			cancel()
			var frow sqliteFrontierLocalRow
			var prow sqlitePendingLocalRow
			var irow sqliteIntervalLocalRow
			var found bool
			var amount int64
			var key string
			var list []string
			var rows []sqliteFrontierLocalRow
			switch operation {
			case "read-frontier":
				frow, found, err = sqliteReadFrontierLocal(tx, fr.ComputerID, fr.ID)
			case "write-frontier":
				amount, err = sqliteWriteFrontierLocal(tx, fr.ComputerID, &fr, fr)
			case "delete-frontier":
				amount, err = sqliteDeleteFrontierLocal(tx, fr.ComputerID, fr)
			case "component-for":
				key, found, err = sqliteComponentForSegmentLocal(tx, fr.ComputerID, id)
			case "component-list":
				list, err = sqliteComponentSegmentIDsLocal(tx, fr.ComputerID, fr.ID)
			case "component-insert":
				amount, err = sqliteInsertComponentSegmentLocal(tx, fr.ComputerID, fr.ID, id)
			case "component-delete":
				amount, err = sqliteDeleteComponentSegmentLocal(tx, fr.ComputerID, fr.ID, id)
			case "read-pending":
				prow, found, err = sqliteReadPendingLocal(tx, pr.ComputerID, pr.ID)
			case "write-pending":
				amount, err = sqliteWritePendingLocal(tx, pr.ComputerID, &pr, pr)
			case "delete-pending":
				amount, err = sqliteDeletePendingLocal(tx, pr.ComputerID, pr)
			case "next-pending":
				prow, found, err = sqliteNextPendingLocal(tx, pr.ComputerID, nil)
			case "read-interval":
				irow, found, err = sqliteReadIntervalLocal(tx, ir.ComputerID, ir.ID)
			case "insert-interval":
				amount, err = sqliteInsertIntervalLocal(tx, ir.ComputerID, ir)
			case "next-interval-ordinal":
				amount, err = sqliteNextIntervalOrdinalLocal(tx)
			case "support-list":
				list, err = sqliteIntervalSegmentIDsLocal(tx, ir.ComputerID, ir.ID)
			case "support-next":
				amount, err = sqliteNextIntervalSegmentOrdinalLocal(tx, ir.ComputerID, ir.ID)
			case "support-append":
				amount, err = sqliteAppendIntervalSegmentLocal(tx, ir.ComputerID, ir.ID, 0, id)
			case "frontier-pre":
				frow, found, err = sqliteFrontierPredecessorLocal(tx, fr.ComputerID, fr.Attribution, fr.Start)
			case "frontier-post":
				frow, found, err = sqliteFrontierSuccessorLocal(tx, fr.ComputerID, fr.Attribution, fr.Start.Add(-time.Second))
			case "reservation":
				rows, err = sqliteFrontierReservationRowsLocal(tx, timer, sqliteReservationWindow{Start: fr.Start})
			case "interval-pre":
				irow, found, err = sqliteIntervalPredecessorLocal(tx, timer, ir.Start)
			case "interval-post":
				irow, found, err = sqliteIntervalSuccessorLocal(tx, timer, ir.Start.Add(-time.Second))
			case "latest":
				irow, found, err = sqliteLatestIntervalLocal(tx, timer)
			}
			if !errors.Is(err, context.Canceled) || found || amount != 0 || key != "" || list != nil || rows != nil || frow != (sqliteFrontierLocalRow{}) || prow != (sqlitePendingLocalRow{}) || irow != (sqliteIntervalLocalRow{}) {
				t.Fatal("cancellation lost exact evidence or zero outputs", err)
			}
			interopSafeError(t, err, f.directory, fr.ID, ir.ID)
			if cleanup := tx.Rollback(); cleanup != nil {
				interopSafeError(t, cleanup, f.directory)
			}
			interopClose(t, c)
			fpiQAReopen(t, f, m, snap)
		})
	}
	t.Run("real-trigger-failure-after-earlier-sibling-and-meta", func(t *testing.T) {
		c, tx := interopOpen(t, f, false, sqliteio.Write)
		d, err := sqliteWriteFrontierLocal(tx, fr.ComputerID, nil, fr)
		if err != nil || d != fpiQAFCharge(t, fr) {
			t.Fatal("ordinary mutation control", err)
		}
		next := metaQANext(t, m)
		next.Revision = bump(m.Revision)
		next.LogicalBytes += d
		if err := sqliteUpdateMeta(tx, m, next); err != nil {
			t.Fatal(err)
		}
		interopDone(t, tx, "CREATE TRIGGER fpi_qa_fail AFTER INSERT ON intervals BEGIN SELECT RAISE(FAIL,'synthetic-fixture-failure'); END")
		d, err = sqliteInsertIntervalLocal(tx, ir.ComputerID, ir)
		var native *sqliteio.Error
		if d != 0 || !errors.As(err, &native) || native.Code != 1811 {
			t.Fatal("native trigger refusal lost evidence/zero delta", err)
		}
		var domain *Error
		if errors.As(err, &domain) && domain.Code == "validation" {
			t.Fatal("non-identity constraint relabeled as identity conflict")
		}
		// FAIL may have changed the row; local zero delta requires caller rollback.
		if cleanup := tx.Rollback(); cleanup != nil {
			interopSafeError(t, cleanup, f.directory)
		}
		interopClose(t, c)
		fpiQAReopen(t, f, m, snap)
	})
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	d, err := sqliteWriteFrontierLocal(tx, fr.ComputerID, nil, fr)
	var native *sqliteio.Error
	if d != 0 || !errors.As(err, &native) || native.Category == sqliteio.Constraint {
		t.Fatal("read-only mutation misclassified", err)
	}
	interopRollback(t, tx)
	if got, found, err := sqliteReadIntervalLocal(tx, ir.ComputerID, ir.ID); err == nil || found || got != (sqliteIntervalLocalRow{}) {
		t.Fatal("ended transaction conflated with absence", err)
	}
	interopClose(t, c)
}

func TestSQLiteFrontierPendingIntervalLocalShapeValidationAndSelectedIdentity(t *testing.T) {
	source := fpiQALegacy(t)
	fr, pr, ir := fpiQARows(t, source)
	f, m, snap := fpiQASeed(t, source)
	for _, defect := range []string{"private-id", "seal-id", "computer", "account", "timezone", "zero-range", "reverse-range"} {
		t.Run(defect, func(t *testing.T) {
			bad := fr
			switch defect {
			case "private-id":
				bad.ID = "fc1:" + strings.Repeat("A", 64)
			case "seal-id":
				bad.ID = "lc1:" + strings.Repeat("a", 64)
			case "computer":
				bad.ComputerID = "invalid-computer"
			case "account":
				bad.Attribution.AccountID = "01"
			case "timezone":
				bad.Attribution.Timezone = "invalid-zone"
			case "zero-range":
				bad.End = bad.Start
			case "reverse-range":
				bad.End = bad.Start.Add(-time.Nanosecond)
			}
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			before, _ := fpiQAAudit(t, tx)
			n, err := sqliteFrontierLocalCharge(bad)
			rdQAPreSQLValidation(t, err)
			if n != 0 {
				t.Fatal("bad frontier charged")
			}
			n, err = sqliteWriteFrontierLocal(tx, fr.ComputerID, nil, bad)
			rdQAPreSQLValidation(t, err)
			if n != 0 {
				t.Fatal("bad frontier mutation")
			}
			p := fpiQAPendingFrom(bad)
			n, err = sqlitePendingLocalCharge(p)
			rdQAPreSQLValidation(t, err)
			if n != 0 {
				t.Fatal("bad pending charged")
			}
			n, err = sqliteWritePendingLocal(tx, fr.ComputerID, nil, p)
			rdQAPreSQLValidation(t, err)
			if n != 0 {
				t.Fatal("bad pending mutation")
			}
			after, _ := fpiQAAudit(t, tx)
			fpiQASame(t, before, after)
			interopRollback(t, tx)
			interopClose(t, c)
		})
	}
	for _, defect := range []string{"uuid", "zero-duration", "noncanonical-duration", "wrong-duration", "negative-ordinal"} {
		t.Run("interval/"+defect, func(t *testing.T) {
			bad := ir
			switch defect {
			case "uuid":
				bad.ID = "FC1:invalid"
			case "zero-duration":
				bad.DurationNS = "0"
			case "noncanonical-duration":
				bad.DurationNS = "020000000000"
			case "wrong-duration":
				bad.DurationNS = "18446744073709551615"
			case "negative-ordinal":
				bad.Ordinal = -1
			}
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			n, err := sqliteIntervalLocalCharge(bad)
			rdQAPreSQLValidation(t, err)
			if n != 0 {
				t.Fatal("bad interval charged")
			}
			n, err = sqliteInsertIntervalLocal(tx, fr.ComputerID, bad)
			rdQAPreSQLValidation(t, err)
			if n != 0 {
				t.Fatal("bad interval mutation")
			}
			interopRollback(t, tx)
			interopClose(t, c)
		})
	}
	for _, family := range []string{"frontier", "pending", "interval"} {
		t.Run("case-insensitive-selected-identity/"+family, func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			table, cols, id, values := "union_frontier", fpiQAFrontierCols, fr.ID, fpiQAFrontierValues(t, fr)
			if family == "pending" {
				table, cols, id, values = "pending_finalization", fpiQAPendingCols, pr.ID, fpiQAPendingValues(t, pr)
			}
			if family == "interval" {
				copy := ir
				copy.ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
				table, cols, id, values = "intervals", fpiQAIntervalCols, copy.ID, fpiQAIntervalValues(t, copy)
			}
			interopDone(t, tx, "ALTER TABLE "+table+" RENAME TO qa_original_"+table)
			columns := fpiQAColumns(cols)
			columns[0] += " COLLATE NOCASE"
			interopDone(t, tx, "CREATE TABLE "+table+"("+strings.Join(columns, ",")+")")
			asQABindFixture(t, tx, table, cols, values)
			if _, found, err := fpiQARead(t, tx, fr.ComputerID, family, id); err != nil || !found {
				t.Fatal("collation positive identity control", err)
			}
			upper := strings.ToUpper(id)
			if upper == id {
				t.Fatal("selected identity perturbation vacuous")
			}
			interopDone(t, tx, "UPDATE "+table+" SET "+fpiQAColumns(cols)[0]+"=?", sqliteio.Text(upper))
			got, found, err := fpiQARead(t, tx, fr.ComputerID, family, id)
			bgQACorrupt(t, err)
			if found || !reflect.DeepEqual(got, fpiQAZero(family)) {
				t.Fatal("selected raw identity mismatch leaked")
			}
			interopRollback(t, tx)
			interopClose(t, c)
		})
	}
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	if owner, found, err := sqliteComponentForSegmentLocal(tx, fr.ComputerID, fpiQAMissing); err != nil || found || owner != "" {
		t.Fatal("reverse membership absence", err)
	}
	if list, err := sqliteComponentSegmentIDsLocal(tx, fr.ComputerID, fr.ID); list != nil {
		t.Fatal("missing component returned list")
	} else {
		bgQACorrupt(t, err)
	}
	if list, err := sqliteIntervalSegmentIDsLocal(tx, ir.ComputerID, ir.ID); list != nil {
		t.Fatal("missing interval returned list")
	} else {
		bgQACorrupt(t, err)
	}
	interopRollback(t, tx)
	interopClose(t, c)
	fpiQAReopen(t, f, m, snap)
}

func TestSQLiteFrontierPendingIntervalLegacyMergeRepresentationBillingOracle(t *testing.T) {
	source := fpiQALegacy(t)
	before, err := json.Marshal(source)
	if err != nil {
		t.Fatal(err)
	}
	in := source.Intervals[0]
	ids := append([]string(nil), in.SegmentIDs...)
	sort.Strings(ids)
	shorterStart := in.Start.In(time.FixedZone("shorter-start", 3600))
	longerStart := in.Start.In(time.FixedZone("longer-start", -3600))
	merged := mergeRanges([]timeRange{{start: longerStart, end: in.End, ids: []string{ids[0]}}, {start: shorterStart, end: in.End.Add(-time.Second), ids: []string{ids[1]}}})
	if len(merged) != 1 || merged[0].start != shorterStart || !merged[0].end.Equal(in.End) {
		t.Fatal("equal-start legacy ordering selected independent minimum UUID boundary")
	}
	earlyEnd := in.End.In(time.FixedZone("earlier-start-end", 3600))
	lateEnd := in.End.In(time.FixedZone("later-start-end", -3600))
	merged = mergeRanges([]timeRange{{start: in.Start.Add(time.Second), end: lateEnd, ids: []string{ids[0]}}, {start: in.Start, end: earlyEnd, ids: []string{ids[1]}}})
	if len(merged) != 1 || merged[0].end != earlyEnd {
		t.Fatal("equal-max-end legacy retention changed endpoint bytes")
	}
	touching := mergeRanges([]timeRange{{start: in.Start, end: in.Start.Add(time.Second)}, {start: in.Start.Add(time.Second), end: in.End}})
	if len(touching) != 1 || touching[0].start != in.Start || touching[0].end != in.End {
		t.Fatal("legacy touching merge semantics")
	}
	hash := fpiQAHash(source.ComputerID, in.Attribution, []string{ids[1], ids[0]})
	if hash != fpiQAHash(source.ComputerID, in.Attribution, []string{ids[0], ids[1]}) {
		t.Fatal("component identity depends on order/boundary owner")
	}
	after, err := json.Marshal(source)
	if err != nil || string(before) != string(after) || !validState(source) {
		t.Fatal("billing oracle mutated original public graph", err)
	}
}

func TestSQLiteFrontierPendingIntervalLocalChildTypedRowsAndScopedOwners(t *testing.T) {
	source := fpiQALegacy(t)
	fr, pr, ir := fpiQARows(t, source)
	f, m, snap := fpiQASeed(t, source)
	ids := source.Intervals[0].SegmentIDs
	for _, family := range []string{"component", "support"} {
		for _, defect := range []string{"reference-null", "reference-kind", "reference-utf8", "ordinal-null", "ordinal-kind", "ordinal-negative"} {
			if family == "component" && strings.HasPrefix(defect, "ordinal") {
				continue
			}
			t.Run(family+"/"+defect, func(t *testing.T) {
				c, tx := interopOpen(t, f, false, sqliteio.Write)
				fpiQAFill(t, tx, fr, pr, ir)
				if family == "component" {
					for _, id := range ids[:2] {
						if _, err := sqliteInsertComponentSegmentLocal(tx, fr.ComputerID, fr.ID, id); err != nil {
							t.Fatal(err)
						}
					}
					list, err := sqliteComponentSegmentIDsLocal(tx, fr.ComputerID, fr.ID)
					if err != nil || len(list) != 2 {
						t.Fatal("positive child kind control", err)
					}
					fpiQAShadow(t, tx, "component_segments", "component_id,segment_id")
					value := sqliteio.Null()
					if defect == "reference-kind" {
						value = sqliteio.Blob([]byte(ids[0]))
					}
					if defect == "reference-utf8" {
						value = sqliteio.Text(string([]byte{0xff}))
					}
					interopDone(t, tx, "UPDATE component_segments SET segment_id=? WHERE segment_id=?", value, sqliteio.Text(list[1]))
					list, err = sqliteComponentSegmentIDsLocal(tx, fr.ComputerID, fr.ID)
					bgQACorrupt(t, err)
					if list != nil {
						t.Fatal("typed component list leaked prefix")
					}
				} else {
					for n, id := range ids[:2] {
						if _, err := sqliteAppendIntervalSegmentLocal(tx, ir.ComputerID, ir.ID, int64(n), id); err != nil {
							t.Fatal(err)
						}
					}
					list, err := sqliteIntervalSegmentIDsLocal(tx, ir.ComputerID, ir.ID)
					if err != nil || len(list) != 2 {
						t.Fatal("positive support kind control", err)
					}
					fpiQAShadow(t, tx, "interval_segments", "interval_id,ordinal,segment_id")
					col, value := "segment_id", sqliteio.Null()
					switch defect {
					case "reference-kind":
						value = sqliteio.Blob([]byte(ids[1]))
					case "reference-utf8":
						value = sqliteio.Text(string([]byte{0xff}))
					case "ordinal-null":
						col = "ordinal"
					case "ordinal-kind":
						col, value = "ordinal", sqliteio.Text("1")
					case "ordinal-negative":
						col, value = "ordinal", sqliteio.Integer(-1)
					}
					interopDone(t, tx, "UPDATE interval_segments SET "+col+"=? WHERE ordinal=1", value)
					list, err = sqliteIntervalSegmentIDsLocal(tx, ir.ComputerID, ir.ID)
					bgQACorrupt(t, err)
					if list != nil {
						t.Fatal("typed support list leaked prefix")
					}
					if defect != "ordinal-null" && defect != "ordinal-negative" {
						next, err := sqliteNextIntervalSegmentOrdinalLocal(tx, ir.ComputerID, ir.ID)
						bgQACorrupt(t, err)
						if next != 0 {
							t.Fatal("invalid selected last leaked next")
						}
					}
				}
				interopRollback(t, tx)
				interopClose(t, c)
				fpiQAReopen(t, f, m, snap)
			})
		}
	}
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	fpiQAFill(t, tx, fr, pr, ir)
	if _, err := sqliteInsertComponentSegmentLocal(tx, fr.ComputerID, fr.ID, ids[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := sqliteAppendIntervalSegmentLocal(tx, ir.ComputerID, ir.ID, 0, ids[0]); err != nil {
		t.Fatal(err)
	}
	if owner, found, err := sqliteComponentForSegmentLocal(tx, asQAForeign, ids[0]); owner != "" || found {
		t.Fatal("foreign reverse owner leaked")
	} else {
		bgQACorrupt(t, err)
	}
	if list, err := sqliteComponentSegmentIDsLocal(tx, asQAForeign, fr.ID); list != nil {
		t.Fatal("foreign component leaked")
	} else {
		bgQACorrupt(t, err)
	}
	if list, err := sqliteIntervalSegmentIDsLocal(tx, asQAForeign, ir.ID); list != nil {
		t.Fatal("foreign support leaked")
	} else {
		bgQACorrupt(t, err)
	}
	if next, err := sqliteNextIntervalSegmentOrdinalLocal(tx, asQAForeign, ir.ID); next != 0 {
		t.Fatal("foreign ordinal leaked")
	} else {
		bgQACorrupt(t, err)
	}
	for _, run := range []func() (int64, error){func() (int64, error) { return sqliteInsertComponentSegmentLocal(tx, asQAForeign, fr.ID, ids[1]) }, func() (int64, error) { return sqliteDeleteComponentSegmentLocal(tx, asQAForeign, fr.ID, ids[0]) }, func() (int64, error) { return sqliteAppendIntervalSegmentLocal(tx, asQAForeign, ir.ID, 1, ids[1]) }} {
		d, err := run()
		bgQACorrupt(t, err)
		if d != 0 {
			t.Fatal("foreign child mutation charged")
		}
	}
	if d, err := sqliteComponentSegmentLocalCharge("lc1:"+strings.Repeat("a", 64), ids[0]); d != 0 {
		t.Fatal("bad local component charge")
	} else {
		bgQAValidation(t, err)
	}
	if d, err := sqliteIntervalSegmentLocalCharge(ir.ID, -1, ids[0]); d != 0 {
		t.Fatal("bad support ordinal charge")
	} else {
		bgQAValidation(t, err)
	}
	interopRollback(t, tx)
	interopClose(t, c)
	fpiQAReopen(t, f, m, snap)
}

// Only the intended logical axis may differ after the exact restoration.
// Canonical group/time projections necessarily change with their source field.
func fpiQAOldAxisWitness(t *testing.T, family, axis string, old, actual []sqliteio.Value) {
	t.Helper()
	frontier := map[string][]int{"id": {0}, "computer": {1, 7}, "account": {2, 7}, "user": {3, 7}, "project": {4, 7}, "task": {5, 7}, "timezone": {6, 7}, "start-sec": {8, 10}, "start-nsec": {9, 10}, "start-json": {10}, "end-sec": {11, 13}, "end-nsec": {12, 13}, "end-json": {13}}
	pending := map[string][]int{"id": {0}, "computer": {1}, "account": {1}, "user": {1}, "project": {1}, "task": {1}, "timezone": {1}, "start-sec": {2, 4}, "start-nsec": {3, 4}, "start-json": {4}, "end-sec": {5, 7}, "end-nsec": {6, 7}, "end-json": {7}}
	allowed := frontier[axis]
	if family == "pending" {
		allowed = pending[axis]
	}
	want := map[int]bool{}
	for _, i := range allowed {
		want[i] = true
	}
	if len(old) != len(actual) || len(want) == 0 {
		t.Fatal("axis witness width/axis invalid")
	}
	for i := range old {
		diff := !reflect.DeepEqual(old[i], actual[i])
		if diff != want[i] {
			t.Fatalf("%s/%s stored column%d difference=%t expected=%t", family, axis, i, diff, want[i])
		}
	}
}
