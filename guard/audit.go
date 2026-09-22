// Package guard layers four cooperating capabilities on top of a GORM DB:
// field permissions, ordered hook chains, nested-transaction savepoints and
// slow-query auditing.
package guard

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"

	"gorm.io/gorm"
)

var errAuditPoolNoTx = errors.New("guard: underlying connection pool does not support transactions")

// EventKind classifies an audited event.
type EventKind string

const (
	// EventSlow is recorded for every statement whose execution time meets the
	// configured slow threshold while auditing is enabled.
	EventSlow EventKind = "slow"
	// EventDenied is recorded when field permissions reject a write.
	EventDenied EventKind = "permission_denied"
	// EventHookAbort is recorded when a hook returns an error and the chain
	// stops before the write reaches the database.
	EventHookAbort EventKind = "hook_abort"
	// EventSavepointRollback is recorded when an inner transaction rolls back
	// to its savepoint while the outer transaction stays alive.
	EventSavepointRollback EventKind = "savepoint_rollback"
)

// Event is one audit entry.
type Event struct {
	Kind    EventKind
	Elapsed time.Duration
	SQL     string
	Args    []interface{}
	Detail  string
	At      time.Time
}

// Auditor collects slow statements and lifecycle traces. It is safe for
// concurrent use.
type Auditor struct {
	mu        sync.Mutex
	events    []Event
	threshold time.Duration
}

// NewAuditor creates an auditor with the given slow-query threshold.
func NewAuditor(threshold time.Duration) *Auditor {
	return &Auditor{threshold: threshold}
}

// SetThreshold updates the slow-query threshold.
func (a *Auditor) SetThreshold(d time.Duration) {
	a.mu.Lock()
	a.threshold = d
	a.mu.Unlock()
}

func (a *Auditor) record(ev Event) {
	a.mu.Lock()
	a.events = append(a.events, ev)
	a.mu.Unlock()
}

// trace records a denied write, hook failure or savepoint rollback. Such
// lifecycle traces are always kept, even when slow auditing is switched off
// for the session.
func (a *Auditor) trace(kind EventKind, detail, sqlText string, args []interface{}) {
	if a == nil {
		return
	}
	a.record(Event{Kind: kind, Detail: detail, SQL: sqlText, Args: append([]interface{}(nil), args...), At: time.Now()})
}

// Events returns a copy of all recorded events in occurrence order.
func (a *Auditor) Events() []Event {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]Event, len(a.events))
	copy(out, a.events)
	return out
}

// SlowStatements returns only threshold-exceeded statement events.
func (a *Auditor) SlowStatements() []Event {
	var out []Event
	for _, ev := range a.Events() {
		if ev.Kind == EventSlow {
			out = append(out, ev)
		}
	}
	return out
}

type ctxKey int

const auditStateKey ctxKey = iota

type auditState struct {
	off bool
}

// WithAuditOff returns a context that suspends slow-query auditing. Statements
// executed while suspended are not back-filled when auditing resumes.
func WithAuditOff(ctx context.Context) context.Context {
	return context.WithValue(ctx, auditStateKey, &auditState{off: true})
}

// WithAuditOn returns a context that explicitly re-enables auditing.
func WithAuditOn(ctx context.Context) context.Context {
	return context.WithValue(ctx, auditStateKey, &auditState{off: false})
}

// auditPool wraps a GORM ConnPool, timing every statement.
type auditPool struct {
	inner   gorm.ConnPool
	auditor *Auditor
}

func auditing(ctx context.Context) bool {
	if v, ok := ctx.Value(auditStateKey).(*auditState); ok {
		return !v.off
	}
	return true
}

func (p *auditPool) note(ctx context.Context, sqlText string, args []interface{}, start time.Time) {
	if p.auditor == nil || !auditing(ctx) {
		return
	}
	p.auditor.mu.Lock()
	threshold := p.auditor.threshold
	p.auditor.mu.Unlock()
	elapsed := time.Since(start)
	if elapsed < threshold {
		return
	}
	p.auditor.record(Event{
		Kind:    EventSlow,
		Elapsed: elapsed,
		SQL:     sqlText,
		Args:    append([]interface{}(nil), args...),
		At:      start,
	})
}

func (p *auditPool) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return p.inner.PrepareContext(ctx, query)
}

// BeginTx keeps transaction support alive through the timing wrapper. The
// returned pool is wrapped again so statements inside a transaction are timed
// too.
func (p *auditPool) BeginTx(ctx context.Context, opts *sql.TxOptions) (gorm.ConnPool, error) {
	if beginner, ok := p.inner.(interface {
		BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	}); ok {
		tx, err := beginner.BeginTx(ctx, opts)
		if err != nil {
			return nil, err
		}
		return &auditPool{inner: tx, auditor: p.auditor}, nil
	}
	beginner, ok := p.inner.(gorm.ConnPoolBeginner)
	if !ok {
		return nil, errAuditPoolNoTx
	}
	txPool, err := beginner.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &auditPool{inner: txPool, auditor: p.auditor}, nil
}

// Commit and Rollback pass through to a wrapped sql.Tx so GORM's Commit and
// Rollback keep working while statements remain timed.
func (p *auditPool) Commit() error {
	if c, ok := p.inner.(interface{ Commit() error }); ok {
		return c.Commit()
	}
	return errAuditPoolNoTx
}

func (p *auditPool) Rollback() error {
	if c, ok := p.inner.(interface{ Rollback() error }); ok {
		return c.Rollback()
	}
	return errAuditPoolNoTx
}

func (p *auditPool) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	start := time.Now()
	res, err := p.inner.ExecContext(ctx, query, args...)
	p.note(ctx, query, args, start)
	return res, err
}

func (p *auditPool) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	start := time.Now()
	rows, err := p.inner.QueryContext(ctx, query, args...)
	p.note(ctx, query, args, start)
	return rows, err
}

func (p *auditPool) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	start := time.Now()
	row := p.inner.QueryRowContext(ctx, query, args...)
	p.note(ctx, query, args, start)
	return row
}
