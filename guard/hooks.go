package guard

import (
	"errors"
	"fmt"
)

// HookPoint identifies one position in a write's lifecycle.
type hookPoint string

const (
	// BeforeCreate runs before an insert reaches the database.
	BeforeCreate hookPoint = "before_create"
	// AfterCreate runs after a successful insert.
	AfterCreate hookPoint = "after_create"
	// BeforeUpdate runs before an update reaches the database.
	BeforeUpdate hookPoint = "before_update"
	// AfterUpdate runs after a successful update.
	AfterUpdate hookPoint = "after_update"
	// BeforeSave runs before both inserts and updates, ahead of the specific
	// before hook.
	BeforeSave hookPoint = "before_save"
	// AfterSave runs after both inserts and updates, after the specific after
	// hook.
	AfterSave hookPoint = "after_save"
)

// HookFunc inspects and mutates the value being written. The same value
// instance is passed through the whole chain, so a field changed by an
// earlier hook is visible to every later hook.
type HookFunc func(ctx *HookContext) error

type registeredHook struct {
	order int
	fn    HookFunc
}

// HookContext is handed to every hook.
type HookContext struct {
	Table    string
	IsCreate bool
	Value    interface{}
	// Read runs a query inside the hook. Tables whose chain is still in flight
	// are blocked so half-finished writes cannot leak out.
	Read func(dest interface{}, table string) error
}

var errUnknownHookPoint = errors.New("guard: unknown hook point")

// RegisterHook appends a hook to a table's chain. Registration order is the
// execution order and cannot be reordered afterwards. Hooks registered on the
// Store run in every session.
func (s *Store) RegisterHook(table string, point hookPoint, fn HookFunc) error {
	switch point {
	case BeforeSave, BeforeCreate, BeforeUpdate, AfterCreate, AfterUpdate, AfterSave:
	default:
		return fmt.Errorf("%w: %s", errUnknownHookPoint, point)
	}
	s.hooksMu.Lock()
	defer s.hooksMu.Unlock()
	tables := s.hooks[table]
	if tables == nil {
		tables = map[hookPoint][]*registeredHook{}
		s.hooks[table] = tables
	}
	chain := tables[point]
	tables[point] = append(chain, &registeredHook{order: len(chain), fn: fn})
	return nil
}

func mergeHooks(a, b []*registeredHook) []*registeredHook {
	if len(b) == 0 {
		return a
	}
	out := make([]*registeredHook, 0, len(a)+len(b))
	out = append(out, a...)
	base := len(a)
	for i, h := range b {
		out = append(out, &registeredHook{order: base + i, fn: h.fn})
	}
	return out
}

func (s *Store) chainFor(sess *Session, table string, point hookPoint) []*registeredHook {
	s.hooksMu.RLock()
	var chain []*registeredHook
	if tables := s.hooks[table]; tables != nil {
		chain = append(chain, tables[point]...)
	}
	s.hooksMu.RUnlock()
	if sess != nil {
		sess.hooksMu.RLock()
		if tables := sess.hooks[table]; tables != nil {
			chain = mergeHooks(chain, tables[point])
		}
		sess.hooksMu.RUnlock()
	}
	return chain
}

// runChain executes hooks in registration order. The first error stops the
// chain; later hooks and the database write never run. All hooks share one
// value instance, so mutations propagate down the chain.
func (s *Store) runChain(sess *Session, frame *txFrame, table string, points []hookPoint, value interface{}, isCreate bool) error {
	hctx := &HookContext{
		Table:    table,
		IsCreate: isCreate,
		Value:    value,
		Read: func(dest interface{}, readTable string) error {
			return s.readFromHook(sess, frame, dest, readTable)
		},
	}
	for _, point := range points {
		for _, h := range s.chainFor(sess, table, point) {
			if err := h.fn(hctx); err != nil {
				s.auditor.trace(EventHookAbort,
					fmt.Sprintf("table=%s point=%s order=%d err=%v", table, point, h.order, err), "", nil)
				return err
			}
		}
	}
	return nil
}

func (s *Store) enterWrite(frame *txFrame, table string) func() {
	s.inflightMu.Lock()
	set := s.inflight[frame]
	if set == nil {
		set = map[string]bool{}
		s.inflight[frame] = set
	}
	set[table] = true
	s.inflightMu.Unlock()
	return func() {
		s.inflightMu.Lock()
		if set := s.inflight[frame]; set != nil {
			delete(set, table)
			if len(set) == 0 {
				delete(s.inflight, frame)
			}
		}
		s.inflightMu.Unlock()
	}
}

func (s *Store) isInFlight(frame *txFrame, table string) bool {
	s.inflightMu.Lock()
	defer s.inflightMu.Unlock()
	for f := frame; f != nil; f = f.parent {
		if set := s.inflight[f]; set != nil && set[table] {
			return true
		}
	}
	return false
}

func (s *Store) readFromHook(sess *Session, frame *txFrame, dest interface{}, table string) error {
	if table == "" {
		table = s.tableName(dest)
	}
	if table != "" && s.isInFlight(frame, table) {
		return fmt.Errorf("%w: %s", ErrInFlightWrite, table)
	}
	gdb := s.gormForRead(sess, frame, table, dest)
	if table != "" {
		gdb = gdb.Table(table)
	}
	return gdb.Find(dest).Error
}
