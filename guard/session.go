package guard

import (
	"context"
	"reflect"
	"sync"

	"gorm.io/gorm"
)

// Session is one conversation with the Store. It carries the temporary
// permission tightenings, session hooks and the current transaction frame.
type Session struct {
	store *Store
	gdb   *gorm.DB
	ctx   context.Context

	permMu sync.RWMutex
	perms  map[string]map[string]FieldRestriction

	hooksMu sync.RWMutex
	hooks   map[string]map[hookPoint][]*registeredHook

	frame *txFrame
}

// WithContext returns a session copy carrying the context. Use WithAuditOff
// / WithAuditOn on the context to suspend or resume slow-query auditing.
func (sess *Session) WithContext(ctx context.Context) *Session {
	cp := sess.clone()
	cp.ctx = ctx
	return cp
}

// Context returns the session context.
func (sess *Session) Context() context.Context {
	if sess.ctx != nil {
		return sess.ctx
	}
	return context.Background()
}

func (sess *Session) clone() *Session {
	cp := &Session{
		store: sess.store,
		gdb:   sess.gdb,
		ctx:   sess.ctx,
		frame: sess.frame,
		perms: map[string]map[string]FieldRestriction{},
		hooks: map[string]map[hookPoint][]*registeredHook{},
	}
	sess.permMu.RLock()
	for t, cols := range sess.perms {
		c := make(map[string]FieldRestriction, len(cols))
		for k, v := range cols {
			c[k] = v
		}
		cp.perms[t] = c
	}
	sess.permMu.RUnlock()
	sess.hooksMu.RLock()
	for t, pts := range sess.hooks {
		cp.hooks[t] = map[hookPoint][]*registeredHook{}
		for p, chain := range pts {
			cp.hooks[t][p] = append([]*registeredHook(nil), chain...)
		}
	}
	sess.hooksMu.RUnlock()
	return cp
}

// TightenField tightens one column for this session only.
func (sess *Session) TightenField(table, column string, r FieldRestriction) *Session {
	sess.permMu.Lock()
	cols := sess.perms[table]
	if cols == nil {
		cols = map[string]FieldRestriction{}
		sess.perms[table] = cols
	}
	cols[column] = r
	sess.permMu.Unlock()
	return sess
}

// RegisterHook registers a session-local hook behind any store-level hooks.
func (sess *Session) RegisterHook(table string, point hookPoint, fn HookFunc) error {
	return sess.store.registerSessionHook(sess, table, point, fn)
}

func (s *Store) registerSessionHook(sess *Session, table string, point hookPoint, fn HookFunc) error {
	switch point {
	case BeforeSave, BeforeCreate, BeforeUpdate, AfterCreate, AfterUpdate, AfterSave:
	default:
		return errUnknownHookPoint
	}
	sess.hooksMu.Lock()
	defer sess.hooksMu.Unlock()
	tables := sess.hooks[table]
	if tables == nil {
		tables = map[hookPoint][]*registeredHook{}
		sess.hooks[table] = tables
	}
	chain := tables[point]
	tables[point] = append(chain, &registeredHook{order: len(chain), fn: fn})
	return nil
}

// gorm returns a GORM handle bound to the current transaction frame (if any)
// and session context.
func (sess *Session) gorm() *gorm.DB {
	gdb := sess.gdb
	if sess.frame != nil && sess.frame.gdb != nil {
		gdb = sess.frame.gdb
	}
	return gdb.Session(&gorm.Session{NewDB: true, Context: sess.Context()})
}

// Create inserts one row or a batch (slice). Both share the same permission
// gate and hook chain.
func (sess *Session) Create(value interface{}) error {
	table, err := sess.store.checkWrite(sess, "", value, opCreate)
	if err != nil {
		return err
	}
	return sess.writeLocked(value, table, true, func(v interface{}) error {
		return sess.gorm().Create(v).Error
	})
}

// CreateTo inserts into an explicit table.
func (sess *Session) CreateTo(table string, value interface{}) error {
	if _, err := sess.store.checkWrite(sess, table, value, opCreate); err != nil {
		return err
	}
	return sess.writeLocked(value, table, true, func(v interface{}) error {
		return sess.gorm().Table(table).Create(v).Error
	})
}

// Updates applies a struct or map update. It passes through the same gate as
// Create.
func (sess *Session) Updates(model interface{}, values interface{}) error {
	table := sess.store.tableName(model)
	if _, err := sess.store.checkWrite(sess, table, values, opUpdate); err != nil {
		return err
	}
	return sess.writeLocked(values, table, false, func(v interface{}) error {
		return sess.gorm().Model(model).Updates(v).Error
	})
}

// UpdateColumn updates one column in an explicit table, e.g. with a where map.
func (sess *Session) UpdateColumn(table string, where map[string]interface{}, column string, value interface{}) error {
	updates := map[string]interface{}{column: value}
	if _, err := sess.store.checkWrite(sess, table, updates, opUpdate); err != nil {
		return err
	}
	return sess.writeLocked(updates, table, false, func(v interface{}) error {
		return sess.gorm().Table(table).Where(where).Update(column, value).Error
	})
}

// writeLocked wraps a physical write with the ordered hook chain and marks the
// table in flight so hook-triggered reads cannot see the unfinished write.
func (sess *Session) writeLocked(value interface{}, table string, isCreate bool, exec func(interface{}) error) error {
	frame := sess.frame
	if frame == nil {
		frame = &txFrame{store: sess.store}
	}
	leave := sess.store.enterWrite(frame, table)
	defer leave()

	before := []hookPoint{BeforeSave}
	after := []hookPoint{AfterSave}
	if isCreate {
		before = append(before, BeforeCreate)
		after = append(after, AfterCreate)
	} else {
		before = append(before, BeforeUpdate)
		after = append([]hookPoint{AfterUpdate}, after...)
	}
	if err := sess.store.runChain(sess, frame, table, before, value, isCreate); err != nil {
		return err
	}
	if err := exec(value); err != nil {
		return err
	}
	return sess.store.runChain(sess, frame, table, after, value, isCreate)
}

// Export loads rows into dest and strips write-only (and session-hidden)
// columns. Read-only columns are exported normally.
func (sess *Session) Export(dest interface{}) error {
	return sess.exportTable("", dest, nil)
}

// ExportTable loads from an explicit table with optional conditions.
func (sess *Session) ExportTable(table string, dest interface{}, where map[string]interface{}) error {
	return sess.exportTable(table, dest, where)
}

func (sess *Session) exportTable(table string, dest interface{}, where map[string]interface{}) error {
	if table == "" {
		table = sess.store.tableName(dest)
	}
	gdb := sess.gorm()
	if table != "" {
		gdb = gdb.Table(table)
	}
	if len(where) > 0 {
		gdb = gdb.Where(where)
	}
	if err := gdb.Find(dest).Error; err != nil {
		return err
	}
	sess.stripWriteOnly(table, dest)
	return nil
}

func (sess *Session) stripWriteOnly(table string, dest interface{}) {
	rv := reflect.ValueOf(dest)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return
	}
	elem := rv.Elem()
	if elem.Kind() == reflect.Slice || elem.Kind() == reflect.Array {
		for i := 0; i < elem.Len(); i++ {
			sess.zeroHidden(elem.Index(i))
		}
		return
	}
	sess.zeroHidden(elem)
}

func (sess *Session) zeroHidden(rv reflect.Value) {
	for rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return
		}
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return
	}
	sch, err := sess.store.parseSchema(rv.Interface())
	if err != nil || sch == nil {
		return
	}
	for _, f := range sch.Fields {
		if !sess.store.canRead(sess, sch.Table, f.DBName, taggedReadable(f)) {
			fv := rv.FieldByIndex(f.StructField.Index)
			if fv.CanSet() {
				fv.Set(reflect.Zero(fv.Type()))
			}
		}
	}
}

func (s *Store) gormForRead(sess *Session, frame *txFrame, table string, dest interface{}) *gorm.DB {
	return sess.gorm()
}
