package tests_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type PermOrg struct {
	ID      uint
	Name    string
	Secret  string `gorm:"perm:wo"`
	Locked  string `gorm:"perm:ro"`
	Members []PermMember `gorm:"foreignKey:OrgID"`
}

type PermMember struct {
	ID    uint
	OrgID uint
	Name  string
	Token string `gorm:"perm:wo"`
}

func openPermAuditDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{
		Audit: &gorm.AuditLog{Threshold: time.Nanosecond},
	})
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	if err := db.AutoMigrate(&PermOrg{}, &PermMember{}); err != nil {
		t.Fatalf("failed to migrate: %v", err)
	}
	return db
}

func auditEventsOfKind(db *gorm.DB, kind string) []gorm.AuditEvent {
	var out []gorm.AuditEvent
	for _, ev := range db.AuditEvents() {
		if ev.Kind == kind {
			out = append(out, ev)
		}
	}
	return out
}

func rawStringColumn(t *testing.T, db *gorm.DB, query string, args ...interface{}) string {
	t.Helper()
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("failed to get sql.DB: %v", err)
	}
	var val string
	row := sqlDB.QueryRow(query, args...)
	if err := row.Scan(&val); err != nil {
		t.Fatalf("raw query %q failed: %v", query, err)
	}
	return val
}

func TestFieldPermissionsWriteAndExport(t *testing.T) {
	db := openPermAuditDB(t)

	// single create with nested (FK) writes: read-only blocked, write-only stored
	org := PermOrg{Name: "core", Secret: "s3cret", Locked: "should-not-write",
		Members: []PermMember{{Name: "m1", Token: "tok1"}}}
	if err := db.Create(&org).Error; err != nil {
		t.Fatalf("create failed: %v", err)
	}

	if got := rawStringColumn(t, db, "SELECT secret FROM perm_orgs WHERE id = ?", org.ID); got != "s3cret" {
		t.Fatalf("write-only field should be stored, got %q", got)
	}
	if got := rawStringColumn(t, db, "SELECT COALESCE(locked, '') FROM perm_orgs WHERE id = ?", org.ID); got != "" {
		t.Fatalf("read-only field should be blocked on write, got %q", got)
	}
	if got := rawStringColumn(t, db, "SELECT token FROM perm_members WHERE org_id = ?", org.ID); got != "tok1" {
		t.Fatalf("nested write-only field should be stored, got %q", got)
	}

	// export: read-only given back, write-only never appears
	var got PermOrg
	if err := db.Preload("Members").First(&got, org.ID).Error; err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if got.Secret != "" {
		t.Fatalf("write-only field leaked into export: %q", got.Secret)
	}
	if len(got.Members) != 1 || got.Members[0].Token != "" {
		t.Fatalf("nested write-only field leaked into export: %+v", got.Members)
	}

	// batch create goes through the same door
	batch := []PermOrg{{Name: "b1", Locked: "x"}, {Name: "b2", Locked: "y"}}
	if err := db.Create(&batch).Error; err != nil {
		t.Fatalf("batch create failed: %v", err)
	}
	for _, b := range batch {
		if got := rawStringColumn(t, db, "SELECT COALESCE(locked, '') FROM perm_orgs WHERE id = ?", b.ID); got != "" {
			t.Fatalf("read-only field should be blocked on batch write, got %q", got)
		}
	}

	// update is blocked too
	if err := db.Model(&PermOrg{}).Where("id = ?", org.ID).Update("locked", "z").Error; err != nil {
		t.Fatalf("update failed: %v", err)
	}
	if got := rawStringColumn(t, db, "SELECT COALESCE(locked, '') FROM perm_orgs WHERE id = ?", org.ID); got != "" {
		t.Fatalf("read-only field should be blocked on update, got %q", got)
	}

	// blocked writes left an audit trail
	if evs := auditEventsOfKind(db, gorm.AuditPermBlocked); len(evs) == 0 {
		t.Fatalf("expected perm_blocked audit events, got none")
	} else if params := evs[0].Params; len(params) == 0 {
		t.Fatalf("perm_blocked event should carry blocked columns")
	}
}

func TestFieldPermissionSessionTighten(t *testing.T) {
	db := openPermAuditDB(t)
	org := PermOrg{Name: "visible", Secret: "s"}
	if err := db.Create(&org).Error; err != nil {
		t.Fatalf("create failed: %v", err)
	}

	// baseline: name is exported
	var before PermOrg
	if err := db.First(&before, org.ID).Error; err != nil || before.Name != "visible" {
		t.Fatalf("baseline export wrong: %+v, %v", before, err)
	}

	// tighten: name hidden from export and blocked from writes on this session
	restricted := db.RestrictFields("Name")
	var after PermOrg
	if err := restricted.First(&after, org.ID).Error; err != nil {
		t.Fatalf("restricted query failed: %v", err)
	}
	if after.Name != "" {
		t.Fatalf("tightened field should be hidden from export, got %q", after.Name)
	}
	if err := restricted.Model(&PermOrg{}).Where("id = ?", org.ID).Update("name", "changed").Error; err != nil {
		t.Fatalf("restricted update failed: %v", err)
	}
	if got := rawStringColumn(t, db, "SELECT name FROM perm_orgs WHERE id = ?", org.ID); got != "visible" {
		t.Fatalf("tightened field should be blocked from writes, got %q", got)
	}

	// tightening recorded in audit
	if evs := auditEventsOfKind(db, gorm.AuditPermTightened); len(evs) == 0 {
		t.Fatalf("expected perm_tightened audit event")
	}

	// outside the tightened session, export works again
	var again PermOrg
	if err := db.First(&again, org.ID).Error; err != nil || again.Name != "visible" {
		t.Fatalf("tightening should be session-scoped: %+v, %v", again, err)
	}
}

func TestHookChainOrderErrorAndVisibility(t *testing.T) {
	db := openPermAuditDB(t)

	var order []string
	var observed string
	db.OnHook(gorm.HookBeforeCreate, func(tx *gorm.DB) error {
		if org, ok := tx.Statement.Dest.(*PermOrg); ok && org.Name == "orig" {
			org.Name = "hooked"
			order = append(order, "first")
		}
		return nil
	})
	db.OnHook(gorm.HookBeforeCreate, func(tx *gorm.DB) error {
		// sees the value changed by the previous hook
		if org, ok := tx.Statement.Dest.(*PermOrg); ok && org.Name == "hooked" {
			observed = org.Name
			order = append(order, "second")
		}
		return nil
	})

	if err := db.Create(&PermOrg{Name: "orig"}).Error; err != nil {
		t.Fatalf("create failed: %v", err)
	}
	if strings.Join(order, ",") != "first,second" {
		t.Fatalf("hooks should run in registration order, got %v", order)
	}
	if observed != "hooked" {
		t.Fatalf("later hook should see earlier hook's change, got %q", observed)
	}

	// a failing hook stops the chain and the write
	hookErr := errors.New("stop here")
	ran := map[string]bool{}
	db.OnHook(gorm.HookBeforeCreate, func(tx *gorm.DB) error {
		if org, ok := tx.Statement.Dest.(*PermOrg); ok && org.Name == "victim" {
			ran["before"] = true
			return hookErr
		}
		return nil
	})
	db.OnHook(gorm.HookBeforeCreate, func(tx *gorm.DB) error {
		if org, ok := tx.Statement.Dest.(*PermOrg); ok && org.Name == "victim" {
			ran["after"] = true
		}
		return nil
	})
	db.OnHook(gorm.HookAfterCreate, func(tx *gorm.DB) error {
		ran["afterCreate"] = true
		return nil
	})

	err := db.Create(&PermOrg{Name: "victim"}).Error
	if !errors.Is(err, hookErr) {
		t.Fatalf("expected hook error, got %v", err)
	}
	if !ran["before"] || ran["after"] || ran["afterCreate"] {
		t.Fatalf("chain should stop at the failing hook: %v", ran)
	}
	var count int64
	db.Model(&PermOrg{}).Where("name = ?", "victim").Count(&count)
	if count != 0 {
		t.Fatalf("failed hook should abort the write, found %d rows", count)
	}
}

func TestNestedTransactionSavepoints(t *testing.T) {
	db := openPermAuditDB(t)

	// inner rollback only rewinds to the savepoint; outer survives and commits
	err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&PermOrg{Name: "outer"}).Error; err != nil {
			return err
		}
		innerErr := tx.Transaction(func(tx2 *gorm.DB) error {
			if err := tx2.Create(&PermOrg{Name: "inner"}).Error; err != nil {
				return err
			}
			return errors.New("rewind to savepoint")
		})
		if innerErr == nil {
			t.Fatalf("expected inner transaction error")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("outer transaction should commit, got %v", err)
	}
	var outerCount, innerCount int64
	db.Model(&PermOrg{}).Where("name = ?", "outer").Count(&outerCount)
	db.Model(&PermOrg{}).Where("name = ?", "inner").Count(&innerCount)
	if outerCount != 1 || innerCount != 0 {
		t.Fatalf("savepoint rollback wrong: outer=%d inner=%d", outerCount, innerCount)
	}

	// inner commits, outer rolls back: everything goes
	err = db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&PermOrg{Name: "outer2"}).Error; err != nil {
			return err
		}
		if err := tx.Transaction(func(tx2 *gorm.DB) error {
			return tx2.Create(&PermOrg{Name: "inner2"}).Error
		}); err != nil {
			return err
		}
		return errors.New("outer rewind")
	})
	if err == nil {
		t.Fatalf("expected outer rollback error")
	}
	var c int64
	db.Model(&PermOrg{}).Where("name IN ?", []string{"outer2", "inner2"}).Count(&c)
	if c != 0 {
		t.Fatalf("outer rollback should remove inner-committed rows too, got %d", c)
	}

	// savepoint rollback left an audit trail, data did not
	if evs := auditEventsOfKind(db, gorm.AuditSavepointRollback); len(evs) == 0 {
		t.Fatalf("expected savepoint_rollback audit events")
	}
	if evs := auditEventsOfKind(db, gorm.AuditSavepoint); len(evs) == 0 {
		t.Fatalf("expected savepoint audit events")
	}
}

func TestSlowQueryAuditToggle(t *testing.T) {
	db := openPermAuditDB(t)
	if err := db.Create(&PermOrg{Name: "audit-target"}).Error; err != nil {
		t.Fatalf("create failed: %v", err)
	}

	slowBefore := len(auditEventsOfKind(db, gorm.AuditSlowQuery))
	if slowBefore == 0 {
		t.Fatalf("expected slow query events above threshold")
	}

	// events carry elapsed, statement and params
	var found bool
	for _, ev := range auditEventsOfKind(db, gorm.AuditSlowQuery) {
		if ev.Elapsed <= 0 || ev.SQL == "" {
			t.Fatalf("slow query event missing elapsed/sql: %+v", ev)
		}
		if len(ev.Params) > 0 {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected at least one slow query event with params")
	}

	// paused session: events are dropped, not backfilled
	paused := db.PauseAudit()
	var tmp PermOrg
	if err := paused.First(&tmp, 1).Error; err != nil {
		t.Fatalf("paused query failed: %v", err)
	}
	if got := len(auditEventsOfKind(db, gorm.AuditSlowQuery)); got != slowBefore {
		t.Fatalf("paused session should not record, got %d want %d", got, slowBefore)
	}

	// resumed: every statement recorded again
	resumed := paused.ResumeAudit()
	if err := resumed.First(&tmp, 1).Error; err != nil {
		t.Fatalf("resumed query failed: %v", err)
	}
	if got := len(auditEventsOfKind(db, gorm.AuditSlowQuery)); got <= slowBefore {
		t.Fatalf("resumed session should record again, got %d want > %d", got, slowBefore)
	}
}

func TestCrossFeatureSavepointPermAudit(t *testing.T) {
	db := openPermAuditDB(t)

	// a permission-blocked write inside a rolled-back savepoint: audit keeps
	// both trails, data leaves nothing behind
	err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&PermOrg{Name: "keeper"}).Error; err != nil {
			return err
		}
		innerErr := tx.Transaction(func(tx2 *gorm.DB) error {
			if err := tx2.Create(&PermOrg{Name: "doomed", Locked: "blocked"}).Error; err != nil {
				return err
			}
			return errors.New("rewind")
		})
		if innerErr == nil {
			t.Fatalf("expected inner error")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("outer should commit: %v", err)
	}

	var count int64
	db.Model(&PermOrg{}).Where("name = ?", "doomed").Count(&count)
	if count != 0 {
		t.Fatalf("rolled-back row should not remain")
	}
	db.Model(&PermOrg{}).Where("name = ?", "keeper").Count(&count)
	if count != 1 {
		t.Fatalf("pre-savepoint row should remain")
	}

	var permBlocked, savepointRollback bool
	for _, ev := range db.AuditEvents() {
		if ev.Kind == gorm.AuditPermBlocked {
			permBlocked = true
		}
		if ev.Kind == gorm.AuditSavepointRollback {
			savepointRollback = true
		}
	}
	if !permBlocked || !savepointRollback {
		t.Fatalf("audit should contain perm_blocked and savepoint_rollback, got %v/%v", permBlocked, savepointRollback)
	}
}
