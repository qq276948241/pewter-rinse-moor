package gorm

const restrictedFieldsKey = "gorm:restricted_fields"

// registerFieldPermCallbacks installs the field permission guards. Write
// guards run before the core create/update callbacks so single and batched
// writes pass through the same door; the read guard runs before the core
// query callback so exports never include hidden columns.
func registerFieldPermCallbacks(db *DB) {
	db.Callback().Create().Before("gorm:create").Register("gorm:perm_guard_create", permWriteGuard)
	db.Callback().Update().Before("gorm:update").Register("gorm:perm_guard_update", permWriteGuard)
	db.Callback().Query().Before("gorm:query").Register("gorm:perm_guard_query", permReadGuard)
}

// tightenedFields returns the session-level restricted field set
func tightenedFields(db *DB) map[string]bool {
	if db.Statement == nil {
		return nil
	}
	if v, ok := db.Statement.Settings.Load(restrictedFieldsKey); ok {
		if set, ok := v.(map[string]bool); ok {
			return set
		}
	}
	return nil
}

// resolveRestrictedDBNames maps restricted field names (struct field names or
// db column names) onto db column names of the current schema
func resolveRestrictedDBNames(db *DB) []string {
	set := tightenedFields(db)
	if len(set) == 0 || db.Statement.Schema == nil {
		return nil
	}
	var names []string
	for name := range set {
		if field := db.Statement.Schema.LookUpField(name); field != nil && field.DBName != "" {
			names = append(names, field.DBName)
		} else {
			names = append(names, name)
		}
	}
	return names
}

// permWriteGuard blocks read-only and session-restricted columns from writes
func permWriteGuard(tx *DB) {
	if tx.Error != nil || tx.Statement.Schema == nil {
		return
	}

	blocked := map[string]bool{}
	for _, field := range tx.Statement.Schema.Fields {
		if field.ReadOnly && field.DBName != "" {
			blocked[field.DBName] = true
		}
	}
	for _, name := range resolveRestrictedDBNames(tx) {
		blocked[name] = true
	}
	if len(blocked) == 0 {
		return
	}

	columns := make([]string, 0, len(blocked))
	for name := range blocked {
		columns = append(columns, name)
	}
	tx.Statement.Omits = append(tx.Statement.Omits, columns...)

	params := make([]interface{}, 0, len(columns))
	for _, c := range columns {
		params = append(params, c)
	}
	tx.recordAudit(AuditEvent{Kind: AuditPermBlocked, SQL: tx.Statement.Table, Params: params})
}

// permReadGuard keeps write-only and session-restricted columns out of exports
func permReadGuard(tx *DB) {
	if tx.Error != nil || tx.Statement.Schema == nil {
		return
	}

	hidden := map[string]bool{}
	for _, field := range tx.Statement.Schema.Fields {
		if field.WriteOnly && field.DBName != "" {
			hidden[field.DBName] = true
		}
	}
	for _, name := range resolveRestrictedDBNames(tx) {
		hidden[name] = true
	}
	if len(hidden) == 0 {
		return
	}

	if len(tx.Statement.Selects) > 0 {
		kept := tx.Statement.Selects[:0]
		for _, sel := range tx.Statement.Selects {
			if field := tx.Statement.Schema.LookUpField(sel); field != nil && hidden[field.DBName] {
				continue
			}
			if !hidden[sel] {
				kept = append(kept, sel)
			}
		}
		tx.Statement.Selects = kept
		return
	}

	columns := make([]string, 0, len(hidden))
	for name := range hidden {
		columns = append(columns, name)
	}
	tx.Statement.Omits = append(tx.Statement.Omits, columns...)
}

// RestrictFields tightens field permissions for this session: the given
// fields (struct field names or db column names) are blocked from writes and
// hidden from exports, including exports that were previously allowed. The
// tightening only lives on the returned session.
func (db *DB) RestrictFields(fields ...string) (tx *DB) {
	tx = db.getInstance()

	set := map[string]bool{}
	if existing := tightenedFields(tx); existing != nil {
		for name := range existing {
			set[name] = true
		}
	}
	for _, f := range fields {
		set[f] = true
	}
	tx.Statement.Settings.Store(restrictedFieldsKey, set)

	params := make([]interface{}, 0, len(fields))
	for _, f := range fields {
		params = append(params, f)
	}
	tx.recordAudit(AuditEvent{Kind: AuditPermTightened, Params: params})
	return
}
