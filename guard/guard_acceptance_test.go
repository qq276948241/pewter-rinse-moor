package guard_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/guard"
)

var dbSerial int64

type Company struct {
	ID    uint
	Name  string
	Code  string `gorm:"->"` // read-only: writes rejected, exports allowed
	Staff []Employee
}

type Employee struct {
	ID        uint
	Name      string
	SSN       string `gorm:"->:false"` // write-only: accepted on write, never exported
	Badge     string `gorm:"->"`       // read-only: writes rejected
	Note      string
	CompanyID uint
}

type Ledger struct {
	ID   uint
	Memo string
}

func newTestStore(t *testing.T, threshold time.Duration) (*guard.Store, *gorm.DB) {
	t.Helper()
	serial := atomic.AddInt64(&dbSerial, 1)
	dsn := fmt.Sprintf("file:guarddb%d?mode=memory&cache=shared", serial)
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.AutoMigrate(&Company{}, &Employee{}, &Ledger{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	auditor := guard.NewAuditor(threshold)
	return guard.Open(db, auditor), db
}

func resetTables(db *gorm.DB) {
	db.Exec("DELETE FROM employees")
	db.Exec("DELETE FROM companies")
	db.Exec("DELETE FROM ledgers")
}

func TestFieldPermissions(t *testing.T) {
	store, db := newTestStore(t, time.Hour)
	resetTables(db)
	sess := store.Session()

	err := sess.Create(&Employee{Name: "ada", SSN: "ssn-1", Badge: "B-1"})
	if !errors.Is(err, guard.ErrReadOnlyField) {
		t.Fatalf("expected ErrReadOnlyField, got %v", err)
	}

	batch := []Employee{
		{Name: "grace", SSN: "ssn-2"},
		{Name: "linus", SSN: "ssn-3", Badge: "B-3"},
	}
	if err := sess.Create(batch); !errors.Is(err, guard.ErrReadOnlyField) {
		t.Fatalf("batch expected ErrReadOnlyField, got %v", err)
	}
	var count int64
	db.Model(&Employee{}).Count(&count)
	if count != 0 {
		t.Fatalf("rejected batch leaked %d rows", count)
	}

	err = sess.Create(&Company{
		Name:  "acme",
		Staff: []Employee{{Name: "nested", Badge: "B-9"}},
	})
	if !errors.Is(err, guard.ErrReadOnlyField) {
		t.Fatalf("nested write expected rejection, got %v", err)
	}

	if err := sess.Create(&Employee{Name: "grace", SSN: "ssn-2"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	var rows []Employee
	if err := sess.Export(&rows); err != nil {
		t.Fatalf("export: %v", err)
	}
	if len(rows) != 1 || rows[0].Name != "grace" {
		t.Fatalf("unexpected export: %+v", rows)
	}
	if rows[0].SSN != "" {
		t.Fatalf("write-only SSN leaked into export: %q", rows[0].SSN)
	}

	if err := sess.Create(&Company{Name: "acme"}); err != nil {
		t.Fatalf("company create: %v", err)
	}
	if err := sess.Updates(&Company{}, map[string]interface{}{"code": "C-1"}); !errors.Is(err, guard.ErrReadOnlyField) {
		t.Fatalf("read-only map update expected rejection, got %v", err)
	}

	tight := store.TightenField("employees", "note", guard.FieldReadOnly)
	if err := tight.Create(&Employee{Name: "locked", Note: "n"}); !errors.Is(err, guard.ErrReadOnlyField) {
		t.Fatalf("tightened session expected rejection, got %v", err)
	}
	if err := store.Session().Create(&Employee{Name: "open", Note: "n"}); err != nil {
		t.Fatalf("other session should not be tightened: %v", err)
	}

	hidden := store.TightenField("employees", "name", guard.FieldHidden)
	var hiddenRows []Employee
	if err := hidden.Export(&hiddenRows); err != nil {
		t.Fatalf("hidden export: %v", err)
	}
	for _, r := range hiddenRows {
		if r.Name != "" {
			t.Fatalf("hidden column leaked after tightening: %q", r.Name)
		}
	}

	var denied int
	for _, ev := range store.Auditor().Events() {
		if ev.Kind == guard.EventDenied {
			denied++
		}
	}
	if denied == 0 {
		t.Fatal("expected permission_denied audit events")
	}
}

func TestHookChain(t *testing.T) {
	store, db := newTestStore(t, time.Hour)
	resetTables(db)

	var calls []string
	store.RegisterHook("employees", guard.BeforeSave, func(c *guard.HookContext) error {
		calls = append(calls, "save-0")
		e := c.Value.(*Employee)
		e.Note = e.Note + "A"
		return nil
	})
	store.RegisterHook("employees", guard.BeforeCreate, func(c *guard.HookContext) error {
		calls = append(calls, "create-1")
		e := c.Value.(*Employee)
		if e.Note != "A" {
			return fmt.Errorf("hook did not see prior mutation, note=%q", e.Note)
		}
		e.Note = e.Note + "B"
		return nil
	})
	store.RegisterHook("employees", guard.AfterCreate, func(c *guard.HookContext) error {
		e := c.Value.(*Employee)
		if e.Note != "AB" {
			return fmt.Errorf("after hook saw %q", e.Note)
		}
		calls = append(calls, "after-2")
		return nil
	})

	sess := store.Session()
	if err := sess.Create(&Employee{Name: "hooked", SSN: "x"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	want := []string{"save-0", "create-1", "after-2"}
	if fmt.Sprint(calls) != fmt.Sprint(want) {
		t.Fatalf("hook order = %v, want %v", calls, want)
	}
	var got Employee
	db.First(&got, "name = ?", "hooked")
	if got.Note != "AB" {
		t.Fatalf("mutations did not reach db: note=%q", got.Note)
	}

	store.RegisterHook("ledgers", guard.BeforeCreate, func(c *guard.HookContext) error {
		return errors.New("boom")
	})
	store.RegisterHook("ledgers", guard.BeforeCreate, func(c *guard.HookContext) error {
		calls = append(calls, "should-not-run")
		return nil
	})
	if err := store.Session().Create(&Ledger{Memo: "m"}); err == nil {
		t.Fatal("expected hook error")
	}
	if len(calls) != 3 {
		t.Fatalf("later hook ran after error: %v", calls)
	}
	var n int64
	db.Model(&Ledger{}).Count(&n)
	if n != 0 {
		t.Fatalf("write happened after hook failure, %d rows", n)
	}
	var aborted int
	for _, ev := range store.Auditor().Events() {
		if ev.Kind == guard.EventHookAbort {
			aborted++
		}
	}
	if aborted == 0 {
		t.Fatal("expected hook_abort audit event")
	}

	store.RegisterHook("companies", guard.BeforeCreate, func(c *guard.HookContext) error {
		var peek []Company
		err := c.Read(&peek, "")
		if !errors.Is(err, guard.ErrInFlightWrite) {
			return fmt.Errorf("expected in-flight block, got %v", err)
		}
		return nil
	})
	if err := store.Session().Create(&Company{Name: "peek"}); err != nil {
		t.Fatalf("in-flight guard check failed: %v", err)
	}
}

func TestSavepoints(t *testing.T) {
	store, db := newTestStore(t, time.Hour)
	resetTables(db)
	sess := store.Session()

	if err := sess.Begin(); err != nil {
		t.Fatalf("outer begin: %v", err)
	}
	if err := sess.Create(&Company{Name: "outer"}); err != nil {
		t.Fatalf("outer create: %v", err)
	}

	if err := sess.Begin(); err != nil {
		t.Fatalf("inner begin: %v", err)
	}
	if !sess.InTransaction() {
		t.Fatal("expected active transaction")
	}
	if err := sess.Create(&Employee{Name: "inner", CompanyID: 1}); err != nil {
		t.Fatalf("inner create: %v", err)
	}
	if err := sess.Rollback(); err != nil {
		t.Fatalf("inner rollback: %v", err)
	}

	// Read through the same session: SQLite blocks other connections while
	// the outer transaction is still open.
	var innerEmps []Employee
	if err := sess.Export(&innerEmps); err != nil {
		t.Fatalf("read after savepoint rollback: %v", err)
	}
	if len(innerEmps) != 0 {
		t.Fatalf("savepoint rollback left %d employees", len(innerEmps))
	}
	var kept []Company
	if err := sess.Export(&kept); err != nil {
		t.Fatalf("read outer row: %v", err)
	}
	if len(kept) != 1 || kept[0].Name != "outer" {
		t.Fatalf("pre-savepoint write lost: %+v", kept)
	}

	if err := sess.Begin(); err != nil {
		t.Fatalf("second inner begin: %v", err)
	}
	if err := sess.Create(&Company{Name: "inner-committed"}); err != nil {
		t.Fatalf("inner create 2: %v", err)
	}
	if err := sess.Commit(); err != nil {
		t.Fatalf("inner commit: %v", err)
	}
	if err := sess.Rollback(); err != nil {
		t.Fatalf("outer rollback: %v", err)
	}
	var afterOuterRollback int64
	db.Model(&Company{}).Count(&afterOuterRollback)
	if afterOuterRollback != 0 {
		t.Fatalf("outer rollback left %d rows", afterOuterRollback)
	}

	var spEvents int
	for _, ev := range store.Auditor().Events() {
		if ev.Kind == guard.EventSavepointRollback {
			spEvents++
		}
	}
	if spEvents == 0 {
		t.Fatal("expected savepoint_rollback audit trace")
	}

	resetTables(db)
	err := sess.Transaction(func(outer *guard.Session) error {
		outer.Create(&Company{Name: "kept"})
		if err := outer.Transaction(func(inner *guard.Session) error {
			inner.Create(&Company{Name: "discarded"})
			return errors.New("abort inner")
		}); err == nil {
			t.Fatal("expected inner error")
		}
		var keptInner []Company
		if err := outer.Export(&keptInner); err != nil {
			t.Fatalf("read within outer tx: %v", err)
		}
		if len(keptInner) != 1 || keptInner[0].Name != "kept" {
			t.Fatalf("after inner rollback expected only 'kept', got %+v", keptInner)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("outer transaction: %v", err)
	}
	var final int64
	db.Model(&Company{}).Count(&final)
	if final != 1 {
		t.Fatalf("committed outer work missing: %d", final)
	}
}

func TestSlowQueryAudit(t *testing.T) {
	store, db := newTestStore(t, time.Nanosecond)
	resetTables(db)

	sess := store.Session()
	if err := sess.Create(&Employee{Name: "slow", SSN: "ssn"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	events := store.Auditor().SlowStatements()
	if len(events) == 0 {
		t.Fatal("expected slow statements above 1ns threshold")
	}
	last := events[len(events)-1]
	if last.Elapsed <= 0 || last.SQL == "" || len(last.Args) == 0 {
		t.Fatalf("slow event missing elapsed/sql/args: %+v", last)
	}

	off := store.Session().WithContext(guard.WithAuditOff(context.Background()))
	before := len(store.Auditor().SlowStatements())
	if err := off.Create(&Employee{Name: "untracked"}); err != nil {
		t.Fatalf("off create: %v", err)
	}
	if got := len(store.Auditor().SlowStatements()); got != before {
		t.Fatalf("off-period statement recorded: before=%d after=%d", before, got)
	}

	on := store.Session().WithContext(guard.WithAuditOn(context.Background()))
	if err := on.Create(&Employee{Name: "tracked"}); err != nil {
		t.Fatalf("on create: %v", err)
	}
	if got := len(store.Auditor().SlowStatements()); got <= before {
		t.Fatalf("statements after re-enable missing: before=%d after=%d", before, got)
	}

	if err := off.Create(&Employee{Badge: "denied"}); !errors.Is(err, guard.ErrReadOnlyField) {
		t.Fatalf("expected denial, got %v", err)
	}
	foundDenied := false
	for _, ev := range store.Auditor().Events() {
		if ev.Kind == guard.EventDenied {
			foundDenied = true
		}
	}
	if !foundDenied {
		t.Fatal("denied write during audit-off left no trace")
	}

	sp := store.Session().WithContext(guard.WithAuditOff(context.Background()))
	if err := sp.Begin(); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := sp.Begin(); err != nil {
		t.Fatalf("savepoint: %v", err)
	}
	sp.Create(&Ledger{Memo: "temp"})
	if err := sp.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if err := sp.Rollback(); err != nil {
		t.Fatalf("outer rollback: %v", err)
	}
	foundSP := false
	for _, ev := range store.Auditor().Events() {
		if ev.Kind == guard.EventSavepointRollback {
			foundSP = true
		}
	}
	if !foundSP {
		t.Fatal("savepoint rollback left no audit trace")
	}
	var ledgers int64
	db.Model(&Ledger{}).Count(&ledgers)
	if ledgers != 0 {
		t.Fatalf("rolled-back data persisted: %d", ledgers)
	}
}
