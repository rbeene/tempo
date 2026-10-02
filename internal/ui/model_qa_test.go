package ui_test

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/terminal"
	"github.com/rbeene/tempo/internal/ui"
	"github.com/rivo/uniseg"
)

const qaUIComputer = "11111111-1111-4111-8111-111111111111"

func qaUISnapshot(revision string, projects ...string) activity.ActivitySnapshot {
	computer := qaUIComputer
	s := activity.ActivitySnapshot{ContractVersion: 1, SnapshotRevision: revision, ComputerID: &computer, ObservedAt: time.Date(2026, 10, 2, 9, 0, 0, 0, time.UTC), Projects: []activity.ProjectActivity{}, ProjectTimers: []activity.ProjectTimer{}, Actors: []activity.Actor{}, Uncertainties: []activity.Uncertainty{}, ClosedIntervals: []activity.Interval{}}
	for _, id := range projects {
		s.ProjectTimers = append(s.ProjectTimers, activity.ProjectTimer{ComputerID: computer, AccountID: "1", ProjectID: id, ProvisionalUnionNS: "10000000000", ConfirmedClosedNS: "5000000000", ActiveActorRefs: []activity.ActorRef{}, WaitingActorRefs: []activity.ActorRef{}, UnresolvedIDs: []string{}})
	}
	return s
}

func qaUIApply(t *testing.T, m *ui.Model, sequence uint64, s activity.ActivitySnapshot) {
	t.Helper()
	if !m.ApplySnapshot(sequence, s) {
		t.Fatalf("rejected fresh shared snapshot sequence=%d revision=%s", sequence, s.SnapshotRevision)
	}
}

func qaUISelected(t *testing.T, m *ui.Model, project string) {
	t.Helper()
	k, ok := m.SelectedKey()
	if !ok || k != (ui.TimerKey{ComputerID: qaUIComputer, AccountID: "1", ProjectID: project}) {
		t.Fatalf("selection=%+v present=%v want project%s", k, ok, project)
	}
}

func TestQAUIModelSequencesNumericRevisionsAndStaleRecovery(t *testing.T) {
	m := ui.NewModel(120, 40)
	qaUIApply(t, m, 1, qaUISnapshot("9", "100"))
	qaUIApply(t, m, 2, qaUISnapshot("10", "100"))
	if m.SnapshotRevision() != "10" {
		t.Fatalf("revision9→10 compared lexically: %s", m.SnapshotRevision())
	}
	for i, bad := range []string{"9", "01", "-1", "", "18446744073709551616"} {
		if m.ApplySnapshot(uint64(3+i), qaUISnapshot(bad, "200")) {
			t.Errorf("accepted lower/noncanonical revision%q", bad)
		}
	}
	same := qaUISnapshot("10", "100")
	same.ProjectTimers[0].ProvisionalUnionNS = "11000000000"
	same.ObservedAt = same.ObservedAt.Add(time.Second)
	qaUIApply(t, m, 8, same)
	frame := strings.Join(m.Render(nil), "\n")
	if !strings.Contains(frame, "00:00:11") {
		t.Errorf("equal-revision elapsed update was discarded: %q", frame)
	}
	if m.ApplySnapshot(7, qaUISnapshot("11", "200")) || m.ApplySnapshot(8, qaUISnapshot("11", "200")) {
		t.Error("older/equal request replaced newer observation")
	}
	m.MarkStale(9, "Local state busy")
	if !m.IsStale() {
		t.Fatal("failed observation was not marked stale")
	}
	if m.ApplySnapshot(8, qaUISnapshot("11", "200")) {
		t.Error("delayed success cleared a newer failure")
	}
	stale := strings.Join(m.Render(nil), "\n")
	if !strings.Contains(strings.ToLower(stale), "stale") || !strings.Contains(stale, "00:00:11") {
		t.Errorf("stale screen lost frozen value/label: %q", stale)
	}
	qaUISelected(t, m, "100")
	qaUIApply(t, m, 10, same)
	if m.IsStale() {
		t.Error("fresh read did not clear stale status")
	}
	m.MarkStale(9, "Delayed failure")
	if m.IsStale() {
		t.Error("older failure overwrote newer success")
	}
}

func TestQAUIModelSelectionSurvivesReorderRemovalAndEmpty(t *testing.T) {
	m := ui.NewModel(80, 24)
	qaUIApply(t, m, 1, qaUISnapshot("1", "100", "200", "300"))
	qaUISelected(t, m, "100")
	m.Move(1)
	qaUISelected(t, m, "200")
	qaUIApply(t, m, 2, qaUISnapshot("2", "300", "200", "100"))
	qaUISelected(t, m, "200")
	qaUIApply(t, m, 3, qaUISnapshot("3", "300", "100"))
	qaUISelected(t, m, "100") // removed ordinal1 falls back to surviving ordinal1
	m.Move(-100)
	qaUISelected(t, m, "300")
	m.Move(100)
	qaUISelected(t, m, "100")
	qaUIApply(t, m, 4, qaUISnapshot("4"))
	if k, ok := m.SelectedKey(); ok {
		t.Errorf("empty snapshot retained phantom selection: %+v", k)
	}
	m.Move(1)
	if _, ok := m.SelectedKey(); ok {
		t.Error("Move invented empty-state selection")
	}
}

func TestQAUIModelSelectionDistinguishesAccountsWithSameProjectID(t *testing.T) {
	m := ui.NewModel(80, 24)
	s := qaUISnapshot("1", "100", "100")
	s.ProjectTimers[1].AccountID = "2"
	qaUIApply(t, m, 1, s)
	m.Move(1)
	want := ui.TimerKey{ComputerID: qaUIComputer, AccountID: "2", ProjectID: "100"}
	if key, ok := m.SelectedKey(); !ok || key != want {
		t.Fatalf("same projectID collapsed distinct accounts: %+v present=%v", key, ok)
	}
	s.ProjectTimers[0], s.ProjectTimers[1] = s.ProjectTimers[1], s.ProjectTimers[0]
	s.SnapshotRevision = "2"
	qaUIApply(t, m, 2, s)
	if key, ok := m.SelectedKey(); !ok || key != want {
		t.Fatalf("selection moved to another account after reorder: %+v present=%v", key, ok)
	}
}

func TestQAUIModelUsesAuthoritativeProjectTimerAndDefensiveCopy(t *testing.T) {
	m := ui.NewModel(120, 40)
	s := qaUISnapshot("1", "100")
	// The shared projection is authoritative even when attribution detail
	// rows contain different totals. Rendering never re-sums those rows.
	s.Projects = []activity.ProjectActivity{{ComputerID: qaUIComputer, Attribution: activity.Attribution{AccountID: "1", ProjectID: "100", TaskID: "3", Timezone: "UTC"}, ProvisionalUnionNS: "99000000000", ConfirmedClosedNS: "88000000000"}}
	s.ProjectTimers[0].ActiveActorRefs = []activity.ActorRef{{Key: activity.ActorKey{ComputerID: qaUIComputer, AgentID: "active"}, Generation: "1"}}
	qaUIApply(t, m, 1, s)
	frame := m.Render(nil)
	text := strings.Join(frame, "\n")
	for _, label := range []string{"Provisional union", "Confirmed closed", "00:00:10", "00:00:05"} {
		if !strings.Contains(text, label) {
			t.Errorf("shared timer label/value%q missing from %q", label, text)
		}
	}
	if strings.Contains(text, "00:01:39") || strings.Contains(text, "00:01:28") {
		t.Errorf("renderer recomputed project timer from attribution rows: %q", text)
	}
	m.Move(1)
	qaUISelected(t, m, "100")
	s.ProjectTimers[0].ProjectID = "999"
	s.ProjectTimers[0].ProvisionalUnionNS = "999000000000"
	s.ProjectTimers[0].ActiveActorRefs[0].Key.AgentID = "changed"
	s.Projects[0].Attribution.ProjectID = "999"
	*s.ComputerID = "changed"
	if got := m.Render(nil); !reflect.DeepEqual(frame, got) {
		t.Errorf("caller mutation changed retained snapshot: before=%q after=%q", frame, got)
	}
	qaUISelected(t, m, "100")
}

func qaUIFrame(t *testing.T, lines []string, columns, rows int) {
	t.Helper()
	if len(lines) > rows {
		t.Fatalf("frame has%d lines for%d rows", len(lines), rows)
	}
	for _, line := range lines {
		if !utf8.ValidString(line) {
			t.Errorf("rendered invalid UTF8: %q", line)
		}
		if uniseg.StringWidth(line) > max(0, columns-1) {
			t.Errorf("frame exceeds safe cell width%d: %q (%d cells)", columns-1, line, uniseg.StringWidth(line))
		}
		for _, r := range line {
			if unicode.IsControl(r) || r >= 0x202a && r <= 0x202e || r >= 0x2066 && r <= 0x2069 || r == 0x061c || r == 0x200e || r == 0x200f || r == 0x2028 || r == 0x2029 {
				t.Errorf("foreign control/bidi survived rendering: U+%04X in%q", r, line)
			}
		}
	}
}

func TestQAUIModelViewportBudgetsAndSelectedRowVisible(t *testing.T) {
	ids := []string{}
	for i := 100; i < 120; i++ {
		ids = append(ids, fmt.Sprint(i))
	}
	for _, size := range [][2]int{{120, 40}, {80, 24}, {40, 12}, {20, 5}, {1, 1}} {
		t.Run(fmt.Sprintf("%dx%d", size[0], size[1]), func(t *testing.T) {
			m := ui.NewModel(size[0], size[1])
			qaUIApply(t, m, 1, qaUISnapshot("1", ids...))
			m.Move(19)
			lines := m.Render(nil)
			qaUIFrame(t, lines, size[0], size[1])
			text := strings.Join(lines, "\n")
			if size[0] >= 40 && (!strings.Contains(text, "119") || !strings.Contains(text, ">")) {
				t.Errorf("selected row/focus not visible without color: %q", text)
			}
			if size[0] == 20 && !strings.Contains(strings.ToLower(text), "too small") {
				t.Errorf("tiny view lacks safe resize guidance: %q", text)
			}
			m.Resize(80, 24)
			qaUISelected(t, m, "119")
			qaUIFrame(t, m.Render(nil), 80, 24)
		})
	}
}

type qaUISafeStyler struct {
	t      *testing.T
	budget int
	calls  int
}

func (s *qaUISafeStyler) Paint(_ terminal.Role, text string) string {
	s.calls++
	qaUIFrame(s.t, []string{text}, s.budget+1, 1)
	return text
}

func TestQAUIModelSanitizesBeforeStylingAndPreservesGraphemes(t *testing.T) {
	for _, columns := range []int{120, 80, 40, 31, 30, 21, 20, 2, 1} {
		t.Run(fmt.Sprint(columns), func(t *testing.T) {
			m := ui.NewModel(columns, 24)
			s := qaUISnapshot("1", "100")
			qaUIApply(t, m, 1, s)
			payload := "VISIBLE \x1b]52;c;SECRET\a\x1bPmalicious\x1b\\\u202e\u2067\u200f\u061c\u2028\t\r\n" + string([]byte{0xff}) + strings.Repeat("Ω\u0301界👩‍💻", 20)
			m.MarkStale(2, payload)
			styler := &qaUISafeStyler{t: t, budget: max(0, columns-1)}
			lines := m.Render(styler)
			qaUIFrame(t, lines, columns, 24)
			text := strings.Join(lines, "\n")
			if strings.Contains(text, "SECRET") || strings.Contains(text, "malicious") {
				t.Errorf("escape string payload survived sanitization: %q", text)
			}
			if columns >= 80 && (!strings.Contains(text, "VISIBLE") || !strings.Contains(text, "Ω\u0301")) {
				t.Errorf("sanitization dropped legitimate foreign text: %q", text)
			}
			if columns >= 40 && styler.calls == 0 {
				t.Error("renderer bypassed shared styling seam")
			}
			for _, line := range lines {
				if strings.Contains(line, "Ω") && strings.Count(line, "Ω") != strings.Count(line, "Ω\u0301") {
					t.Errorf("clipping split combining cluster: %q", line)
				}
				if strings.Contains(line, "👩") && strings.Count(line, "👩") != strings.Count(line, "👩‍💻") {
					t.Errorf("clipping split emoji ZWJ cluster: %q", line)
				}
			}
		})
	}
}

func TestQAUIModelInitialFailureAndEmptyRemainDistinct(t *testing.T) {
	m := ui.NewModel(80, 24)
	initial := strings.ToLower(strings.Join(m.Render(nil), "\n"))
	if !strings.Contains(initial, "loading") && !strings.Contains(initial, "unavailable") {
		t.Errorf("unobserved state pretends known empty: %q", initial)
	}
	m.MarkStale(1, "State unavailable")
	failed := strings.ToLower(strings.Join(m.Render(nil), "\n"))
	if !strings.Contains(failed, "unavailable") || strings.Contains(failed, "00:00:00") {
		t.Errorf("initial failure rendered fake zero: %q", failed)
	}
	qaUIApply(t, m, 2, qaUISnapshot("0"))
	empty := strings.ToLower(strings.Join(m.Render(nil), "\n"))
	if !strings.Contains(empty, "no activity") || m.IsStale() {
		t.Errorf("successful empty state lacks distinct meaning: %q", empty)
	}
}

func TestQAUIModelInvalidDurationsAreUnavailable(t *testing.T) {
	for _, bad := range []string{"-1", "01", "garbage", "18446744073709551616"} {
		t.Run(bad, func(t *testing.T) {
			m := ui.NewModel(120, 40)
			s := qaUISnapshot("1", "100")
			s.ProjectTimers[0].ProvisionalUnionNS = bad
			s.ProjectTimers[0].ConfirmedClosedNS = bad
			qaUIApply(t, m, 1, s)
			text := strings.ToLower(strings.Join(m.Render(nil), "\n"))
			if !strings.Contains(text, "unavailable") || strings.Contains(text, "00:00:00") {
				t.Errorf("invalid duration coerced to zero/success: %q", text)
			}
		})
	}
}
