package gorm

import (
	"sync"
	"time"
)

// Audit event kinds recorded into the AuditLog
const (
	// AuditSlowQuery is recorded when a statement runs longer than the audit threshold
	AuditSlowQuery = "slow_query"
	// AuditPermBlocked is recorded when a field permission blocks columns from a write
	AuditPermBlocked = "perm_blocked"
	// AuditPermTightened is recorded when a session tightens field permissions
	AuditPermTightened = "perm_tightened"
	// AuditSavepoint is recorded when a savepoint is created
	AuditSavepoint = "savepoint"
	// AuditSavepointRollback is recorded when a transaction rolls back to a savepoint
	AuditSavepointRollback = "savepoint_rollback"
)

const auditPausedKey = "gorm:audit_paused"

// AuditEvent is a single entry in the AuditLog
type AuditEvent struct {
	Kind    string
	SQL     string
	Params  []interface{}
	Elapsed time.Duration
	Err     error
	Time    time.Time
}

// AuditLog collects audit events for a *gorm.DB. It is safe for concurrent use.
type AuditLog struct {
	// Threshold is the slow query threshold; statements taking at least this
	// long are recorded as AuditSlowQuery events. Zero records every statement.
	Threshold time.Duration

	mu     sync.Mutex
	events []AuditEvent
}

func (l *AuditLog) append(ev AuditEvent) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if ev.Time.IsZero() {
		ev.Time = time.Now()
	}
	l.events = append(l.events, ev)
}

// Events returns a copy of all recorded events in recording order
func (l *AuditLog) Events() []AuditEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]AuditEvent, len(l.events))
	copy(out, l.events)
	return out
}

// Reset drops all recorded events
func (l *AuditLog) Reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = nil
}

// recordAudit appends an event to the configured AuditLog unless auditing is
// paused for this session
func (db *DB) recordAudit(ev AuditEvent) {
	if db == nil || db.Config == nil || db.Config.Audit == nil {
		return
	}
	if db.Statement != nil {
		if paused, ok := db.Statement.Settings.Load(auditPausedKey); ok {
			if p, ok := paused.(bool); ok && p {
				return
			}
		}
	}
	db.Config.Audit.append(ev)
}

// PauseAudit temporarily disables audit recording for this session; events
// occurring while paused are dropped and not backfilled
func (db *DB) PauseAudit() (tx *DB) {
	tx = db.getInstance()
	tx.Statement.Settings.Store(auditPausedKey, true)
	return
}

// ResumeAudit re-enables audit recording for this session
func (db *DB) ResumeAudit() (tx *DB) {
	tx = db.getInstance()
	tx.Statement.Settings.Store(auditPausedKey, false)
	return
}

// AuditEvents returns the events recorded on this db's AuditLog, or nil when
// auditing is not configured
func (db *DB) AuditEvents() []AuditEvent {
	if db == nil || db.Config == nil || db.Config.Audit == nil {
		return nil
	}
	return db.Config.Audit.Events()
}
