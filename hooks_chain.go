package gorm

import (
	"fmt"
	"sync/atomic"
)

// HookPoint identifies a position in the hook chain
type HookPoint string

// Supported hook chain positions
const (
	HookBeforeCreate HookPoint = "before_create"
	HookAfterCreate  HookPoint = "after_create"
	HookBeforeUpdate HookPoint = "before_update"
	HookAfterUpdate  HookPoint = "after_update"
	HookBeforeSave   HookPoint = "before_save"
	HookAfterSave    HookPoint = "after_save"
	HookBeforeDelete HookPoint = "before_delete"
	HookAfterDelete  HookPoint = "after_delete"
	HookBeforeQuery  HookPoint = "before_query"
	HookAfterQuery   HookPoint = "after_query"
)

var hookChainCounter int64

// OnHook registers fn into the hook chain at the given point. Hooks stack:
// hooks registered at the same point run in registration order. A hook that
// returns an error stops the chain — later hooks and the underlying database
// operation are skipped. Hooks share the same statement, so field changes
// made by one hook are visible to the hooks that run after it, and queries
// issued from inside a hook run on the same session/transaction.
func (db *DB) OnHook(point HookPoint, fn func(*DB) error) *DB {
	name := fmt.Sprintf("gorm:hookchain:%s:%d", point, atomic.AddInt64(&hookChainCounter, 1))
	wrapper := func(tx *DB) {
		if tx.Error != nil {
			return
		}
		tx.AddError(fn(tx))
	}

	switch point {
	case HookBeforeCreate:
		db.Callback().Create().Before("gorm:create").Register(name, wrapper)
	case HookAfterCreate:
		db.Callback().Create().After("gorm:create").Register(name, wrapper)
	case HookBeforeUpdate:
		db.Callback().Update().Before("gorm:update").Register(name, wrapper)
	case HookAfterUpdate:
		db.Callback().Update().After("gorm:update").Register(name, wrapper)
	case HookBeforeSave:
		db.Callback().Create().Before("gorm:create").Register(name+":create", wrapper)
		db.Callback().Update().Before("gorm:update").Register(name+":update", wrapper)
	case HookAfterSave:
		db.Callback().Create().After("gorm:create").Register(name+":create", wrapper)
		db.Callback().Update().After("gorm:update").Register(name+":update", wrapper)
	case HookBeforeDelete:
		db.Callback().Delete().Before("gorm:delete").Register(name, wrapper)
	case HookAfterDelete:
		db.Callback().Delete().After("gorm:delete").Register(name, wrapper)
	case HookBeforeQuery:
		db.Callback().Query().Before("gorm:query").Register(name, wrapper)
	case HookAfterQuery:
		db.Callback().Query().After("gorm:query").Register(name, wrapper)
	default:
		db.AddError(fmt.Errorf("unsupported hook point: %s", point))
	}
	return db
}
