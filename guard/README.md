# guard — hardened persistence layer

`guard` wraps an initialized `*gorm.DB` and adds four cooperating
capabilities. All writes and exports should go through a `guard.Session` so
the four pieces stay in sync.

## Field permissions

Fields are tagged with the GORM permission syntax, interpreted by `guard`:

- `gorm:"->"` — read-only: writes carrying a value are rejected; exports
  return the column.
- `gorm:"->:false"` — write-only: writes are accepted; the column is zeroed
  out of every export.
- `gorm:"-"` / `gorm:"-:all"` — neither writable nor exportable.

Single-row, batch (`[]Model`), nested-association and `map` writes all pass
through one gate (`Store.checkWrite`), so a single offending row rejects the
batch before any SQL is issued.

`store.TightenField(table, column, FieldReadOnly|FieldHidden)` returns a
session whose permissions are temporarily tighter. Other sessions keep the
old rules, and `FieldHidden` also claws back previously allowed exports.

## Hook chains

Register hooks at `BeforeSave`, `BeforeCreate`, `BeforeUpdate`,
`AfterCreate`, `AfterUpdate` and `AfterSave` with `RegisterHook`. Hooks run
in strict registration order (store hooks first, then session hooks). Every
hook receives the same value instance, so a mutation by an earlier hook is
visible to later hooks and reaches the database. A hook error aborts the
chain: later hooks and the write never run, and the event is audited.

`HookContext.Read` lets a hook query the database, but reads of the table
whose chain is still in flight fail with `ErrInFlightWrite` — a half-finished
write can never leak out, even from another hook.

## Transactions and savepoints

`Session.Begin` inside an open transaction issues a `SAVEPOINT` instead of a
second database transaction. Inner `Rollback` runs `ROLLBACK TO SAVEPOINT`
and leaves the outer transaction and all earlier writes intact; inner
`Commit` releases the savepoint while the outer transaction can still roll
the whole unit back. `Session.Transaction` nests the same way and turns
errors/panics into rollback.

## Slow-query auditing

`NewAuditor(threshold)` records every statement at or above the threshold
with elapsed time, SQL and arguments. Suspend/resume auditing per session
with `guard.WithAuditOff(ctx)` / `guard.WithAuditOn(ctx)`: statements run
while suspended are recorded neither at the time nor back-filled later.

Lifecycle traces (`permission_denied`, `hook_abort`, `savepoint_rollback`)
are always recorded, even while slow auditing is off — so a savepoint
rollback and a permission-rejected write are always traceable, while rolled
back data never persists.
