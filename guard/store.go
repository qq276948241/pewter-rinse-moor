package guard

import (
	"context"
	"errors"
	"reflect"
	"sync"

	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

// ErrReadOnlyField is returned when a write carries a value for a read-only
// field.
var ErrReadOnlyField = errors.New("guard: write rejected, read-only field carries a value")

// ErrInFlightWrite is returned when a query issued from inside a hook tries to
// read a table whose hook chain has not finished, i.e. data that is still a
// half-finished write.
var ErrInFlightWrite = errors.New("guard: cannot query a table from inside its own unfinished hook chain")

// FieldRestriction describes a session-scoped tightening on top of the field
// tags.
type FieldRestriction int

const (
	// FieldDefault keeps the behaviour declared by struct tags.
	FieldDefault FieldRestriction = iota
	// FieldReadOnly tightens a field to read-only for the session, even if it
	// was previously writable.
	FieldReadOnly
	// FieldHidden tightens a field so it is neither writable nor exported for
	// the session.
	FieldHidden
)

type fieldRule struct {
	write bool
	read  bool
}

type tableRules struct {
	mu    sync.RWMutex
	rules map[string]fieldRule
}

// Store is the hardened persistence layer built on top of a *gorm.DB.
type Store struct {
	db       *gorm.DB
	basePool gorm.ConnPool
	auditor  *Auditor

	permMu sync.RWMutex
	// perms holds session tightenings keyed by table name, then column name.
	perms map[string]map[string]FieldRestriction

	hooksMu sync.RWMutex
	hooks   map[string]map[hookPoint][]*registeredHook

	schemaCache sync.Map // reflect.Type -> *schema.Schema

	inflightMu sync.Mutex
	// inflight tracks unfinished hook chains. Each transaction level owns its
	// own set so inner savepoints do not poison the outer frame.
	inflight map[*txFrame]map[string]bool
}

// Open wraps an already initialized GORM DB with field permissions, hook
// chains, savepoint transactions and slow-query auditing.
func Open(db *gorm.DB, auditor *Auditor) *Store {
	db.Config.SkipDefaultTransaction = true
	s := &Store{
		db:       db,
		auditor:  auditor,
		perms:    map[string]map[string]FieldRestriction{},
		hooks:    map[string]map[hookPoint][]*registeredHook{},
		inflight: map[*txFrame]map[string]bool{},
	}
	s.basePool = db.Config.ConnPool
	if auditor != nil {
		pool := &auditPool{inner: db.Config.ConnPool, auditor: auditor}
		db.Config.ConnPool = pool
		if db.Statement != nil {
			db.Statement.ConnPool = pool
		}
		s.basePool = pool
	}
	return s
}

// DB exposes the underlying GORM DB. Callers should route writes and exports
// through the Store so all four capabilities stay in sync.
func (s *Store) DB() *gorm.DB { return s.db }

// Auditor returns the attached auditor, if any.
func (s *Store) Auditor() *Auditor { return s.auditor }

func (s *Store) parseSchema(dest interface{}) (*schema.Schema, error) {
	rv := reflect.ValueOf(dest)
	for rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		rv = reflect.New(rv.Type().Elem()).Elem()
		if rv.Kind() == reflect.Ptr {
			rv = reflect.New(rv.Type().Elem())
		}
	}
	if rv.Kind() != reflect.Struct {
		return nil, nil
	}
	key := rv.Type()
	if cached, ok := s.schemaCache.Load(key); ok {
		return cached.(*schema.Schema), nil
	}
	parsed, err := schema.Parse(reflect.New(key).Interface(), &s.schemaCache, s.db.NamingStrategy)
	if err != nil {
		return nil, err
	}
	s.schemaCache.Store(key, parsed)
	s.schemaCache.Store(parsed.Table, parsed)
	return parsed, nil
}

// tableName resolves the table name for a model value, honoring explicit
// TableName methods.
func (s *Store) tableName(dest interface{}) string {
	sch, err := s.parseSchema(dest)
	if err != nil || sch == nil {
		return ""
	}
	return sch.Table
}

// TightenField tightens one column of one table for the returned session. The
// Store itself is unchanged: other sessions keep the old permissions.
func (s *Store) TightenField(table, column string, r FieldRestriction) *Session {
	return s.Session().TightenField(table, column, r)
}

func (s *Store) snapshotPerms() map[string]map[string]FieldRestriction {
	s.permMu.RLock()
	defer s.permMu.RUnlock()
	out := make(map[string]map[string]FieldRestriction, len(s.perms))
	for t, cols := range s.perms {
		cp := make(map[string]FieldRestriction, len(cols))
		for c, r := range cols {
			cp[c] = r
		}
		out[t] = cp
	}
	return out
}

func (s *Store) setPerm(table, column string, r FieldRestriction) {
	s.permMu.Lock()
	defer s.permMu.Unlock()
	cols := s.perms[table]
	if cols == nil {
		cols = map[string]FieldRestriction{}
		s.perms[table] = cols
	}
	cols[column] = r
}

// canWrite reports whether a column is writable under tags + session rules.
func (s *Store) canWrite(sess *Session, table, column string, taggedWritable bool) bool {
	r := FieldDefault
	s.permMu.RLock()
	if cols, ok := s.perms[table]; ok {
		r = cols[column]
	}
	s.permMu.RUnlock()
	if sess != nil {
		if cols, ok := sess.perms[table]; ok {
			if cr, exists := cols[column]; exists {
				r = cr
			}
		}
	}
	switch r {
	case FieldReadOnly, FieldHidden:
		return false
	}
	return taggedWritable
}

// canRead reports whether a column may appear in exported results.
func (s *Store) canRead(sess *Session, table, column string, taggedReadable bool) bool {
	r := FieldDefault
	s.permMu.RLock()
	if cols, ok := s.perms[table]; ok {
		r = cols[column]
	}
	s.permMu.RUnlock()
	if sess != nil {
		if cols, ok := sess.perms[table]; ok {
			if cr, exists := cols[column]; exists {
				r = cr
			}
		}
	}
	if r == FieldHidden {
		return false
	}
	return taggedReadable
}

// Session opens a new working session.
func (s *Store) Session() *Session {
	return &Session{
		store: s,
		gdb:   s.db.Session(&gorm.Session{NewDB: true, Context: context.Background()}),
		perms: map[string]map[string]FieldRestriction{},
		hooks: map[string]map[hookPoint][]*registeredHook{},
	}
}
