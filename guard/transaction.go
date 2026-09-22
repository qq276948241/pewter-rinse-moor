package guard

import (
	"errors"
	"fmt"
	"sync/atomic"

	"gorm.io/gorm"
)

// txFrame is one nesting level of a transaction. Only the outermost frame
// owns a real database transaction; inner frames are savepoints.
type txFrame struct {
	store     *Store
	parent    *txFrame
	gdb       *gorm.DB
	savepoint string
	depth     int
	done      bool
}

var spCounter int64

func savepointName(depth int) string {
	return fmt.Sprintf("guard_sp_%d_%d", depth, atomic.AddInt64(&spCounter, 1))
}

// Begin starts a transaction. A Begin inside an existing transaction on the
// same session becomes a SAVEPOINT instead of a second database transaction.
func (sess *Session) Begin() error {
	if sess.frame != nil {
		name := savepointName(sess.frame.depth + 1)
		if err := sess.frame.gdb.Exec("SAVEPOINT " + name).Error; err != nil {
			return err
		}
		sess.frame = &txFrame{
			store:     sess.store,
			parent:    sess.frame,
			gdb:       sess.frame.gdb,
			savepoint: name,
			depth:     sess.frame.depth + 1,
		}
		return nil
	}
	root := sess.gdb.Session(&gorm.Session{NewDB: true})
	root.Statement.ConnPool = sess.store.basePool
	tx := root.Begin()
	if tx.Error != nil {
		return tx.Error
	}
	sess.frame = &txFrame{store: sess.store, gdb: tx, depth: 1}
	return nil
}

// Commit releases a savepoint for inner frames or commits the real
// transaction for the outermost frame. The outer transaction can still be
// rolled back later while it remains open.
func (sess *Session) Commit() error {
	frame := sess.frame
	if frame == nil {
		return errors.New("guard: commit without begin")
	}
	if frame.done {
		return errors.New("guard: transaction frame already finished")
	}
	if frame.parent != nil {
		if err := frame.gdb.Exec("RELEASE SAVEPOINT " + frame.savepoint).Error; err != nil {
			return err
		}
		frame.done = true
		sess.frame = frame.parent
		return nil
	}
	frame.done = true
	err := frame.gdb.Commit().Error
	sess.frame = nil
	return err
}

// Rollback of an inner frame only retreats to its savepoint: the outer
// transaction stays alive and everything written before the savepoint is
// preserved. The outermost frame rolls back the whole transaction.
func (sess *Session) Rollback() error {
	frame := sess.frame
	if frame == nil {
		return errors.New("guard: rollback without begin")
	}
	if frame.done {
		return errors.New("guard: transaction frame already finished")
	}
	if frame.parent != nil {
		if err := frame.gdb.Exec("ROLLBACK TO SAVEPOINT " + frame.savepoint).Error; err != nil {
			return err
		}
		sess.store.auditor.trace(EventSavepointRollback,
			fmt.Sprintf("savepoint=%s depth=%d", frame.savepoint, frame.depth),
			"ROLLBACK TO SAVEPOINT "+frame.savepoint, nil)
		frame.done = true
		sess.frame = frame.parent
		return nil
	}
	frame.done = true
	err := frame.gdb.Rollback().Error
	sess.frame = nil
	return err
}

// Transaction runs fn inside a Begin/Commit pair, turning panics and returned
// errors into Rollback. Nesting it produces savepoints.
func (sess *Session) Transaction(fn func(tx *Session) error) error {
	if err := sess.Begin(); err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed && sess.frame != nil && !sess.frame.done {
			_ = sess.Rollback()
		}
	}()
	if err := fn(sess); err != nil {
		return err
	}
	if err := sess.Commit(); err != nil {
		return err
	}
	committed = true
	return nil
}

// InTransaction reports whether the session currently holds an open
// transaction or savepoint frame.
func (sess *Session) InTransaction() bool {
	return sess.frame != nil
}
